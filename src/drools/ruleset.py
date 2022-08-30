import errno
import json
import logging
import os
from dataclasses import dataclass, field
from typing import ClassVar

import jpy
import jpyutil

from .exceptions import RulesetNotFoundError
from .rule import Rule

DEFAULT_JAR = "jars/drools-yaml-rules-durable-rest-1.0.0-SNAPSHOT-runner.jar"
logger = logging.getLogger(__name__)


def _to_json(obj):
    if isinstance(obj, dict):
        return json.dumps(obj)
    return obj


@dataclass
class SingleMatchData:
    _d: dict


@dataclass
class Matches:
    _m: dict = None
    m: SingleMatchData = None


@dataclass
class Ruleset:
    name: str
    serialized_ruleset: str
    _rules: dict = field(init=False, repr=False, default_factory=dict)
    _session_id: int = field(init=False, repr=False, default=None)

    def __post_init__(self):
        RulesetCollection.add(self)

    def add_rule(self, rule: Rule) -> None:
        self._rules[rule.name] = rule

    def start_session(self) -> int:
        if self._session_id:
            return self._session_id

        self._jpy_instance = self._make_jpy_instance()
        self._session_id = self._api.createRuleset(
            self.name, self.serialized_ruleset
        )
        return self._session_id

    def assert_event(self, serialized_fact: str):
        return self._process_response(
            self._api.assertEvent(self._session_id, serialized_fact),
            serialized_fact,
        )

    def assert_fact(self, serialized_fact: str):
        return self._process_response(
            self._api.assertFact(self._session_id, serialized_fact),
            serialized_fact,
        )

    def retract_fact(self, serialized_fact: str):
        return self._process_response(
            self._api.retractFact(self._session_id, serialized_fact),
            serialized_fact,
        )

    def get_facts(self):
        return self._api.getFacts(self._session_id)

    def get_pending_events(self):
        pass

    def _start_action_for_state(self):
        resp = self._api.advanceState()
        if resp is None:
            return None
        return ('{ "sid":"0", "id":"sid-0", "$s":1}', resp, 1)

    def _complete_and_start_action(self):
        resp = self._api.advanceState()
        return resp

    def _process_response(self, result: int, serialized_msg: str):
        while (response := self._start_action_for_state()) is not None:
            _, payload, _ = response
            matches = json.loads(payload)
            for key, value in matches.items():
                if key in self._rules:
                    if len(value.keys()) == 1 and "m" in value.keys():
                        single_match = SingleMatchData(_d=value["m"])
                    else:
                        single_match = None
                    res = Matches(_m=value, m=single_match)
                    self._rules[key].callback(res)

    def _make_jpy_instance(self):
        jar_file_path = os.environ.get("DROOLS_JPY_CLASSPATH")
        if jar_file_path is None:
            package_dir = os.path.dirname(os.path.realpath(__file__))
            jar_file_path = os.path.join(package_dir, DEFAULT_JAR)

        if not os.path.exists(jar_file_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), jar_file_path
            )

        jpyutil.init_jvm(jvm_maxmem="512M", jvm_classpath=[jar_file_path])

        JpyDurableRulesEngine_JavaAPI = jpy.get_type(
            "org.drools.yaml.durable.jpy.JpyDurableRulesEngine"
        )
        self._api = JpyDurableRulesEngine_JavaAPI()


@dataclass
class RulesetCollection:
    __cached_objects: ClassVar[dict[str, Rule]] = {}

    @classmethod
    def add(cls, ruleset: Ruleset):
        cls.__cached_objects[ruleset.name] = ruleset

    @classmethod
    def get(cls, ruleset_name: str) -> Ruleset:
        if ruleset_name not in cls.__cached_objects:
            raise RulesetNotFoundError(
                "Ruleset " + ruleset_name + " not found"
            )

        return cls.__cached_objects[ruleset_name]

    @classmethod
    def start_sessions(cls):
        for obj in cls.__cached_objects.values():
            obj.start_session()


def post(ruleset_name: str, serialized_event: str):
    return RulesetCollection.get(ruleset_name).assert_event(
        _to_json(serialized_event)
    )


def assert_event(ruleset_name: str, serialized_event: str):
    return RulesetCollection.get(ruleset_name).assert_event(
        _to_json(serialized_event)
    )


def assert_fact(ruleset_name: str, serialized_fact: str):
    return RulesetCollection.get(ruleset_name).assert_fact(
        _to_json(serialized_fact)
    )


def retract_fact(ruleset_name: str, serialized_fact: str):
    return RulesetCollection.get(ruleset_name).retract_fact(
        _to_json(serialized_fact)
    )


def get_facts(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_facts()


def get_pending_events(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_pending_events()
