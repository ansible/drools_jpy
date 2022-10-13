import errno
import json
import logging
import os
from dataclasses import dataclass, field
from typing import ClassVar, Dict

import jpyutil

from .exceptions import RuleNotFoundError, RulesetNotFoundError
from .rule import Rule

DEFAULT_JAR = "jars/drools-yaml-rules-durable-rest-1.0.0-SNAPSHOT-runner.jar"
DEFAULT_DROOLS_CLASS = "org.drools.yaml.core.jpy.AstRulesEngine"

logger = logging.getLogger(__name__)


def _make_jpy_instance():
    jar_file_path = os.environ.get("DROOLS_JPY_CLASSPATH")
    if jar_file_path is None:
        package_dir = os.path.dirname(os.path.realpath(__file__))
        jar_file_path = os.path.join(package_dir, DEFAULT_JAR)

    if not os.path.exists(jar_file_path):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), jar_file_path
        )

    jpyutil.init_jvm(jvm_maxmem="512M", jvm_classpath=[jar_file_path])

    import jpy

    return jpy.get_type(DEFAULT_DROOLS_CLASS)()


def _to_json(obj):
    if isinstance(obj, dict):
        return json.dumps(obj)
    return obj


def _from_json(obj):
    if isinstance(obj, str):
        return json.loads(obj)
    return obj


@dataclass(frozen=True)
class Matches:
    data: dict = None


@dataclass
class Ruleset:
    name: str
    serialized_ruleset: str
    _rules: dict = field(init=False, repr=False, default_factory=dict)
    _session_id: int = field(init=False, repr=False, default=None)
    _api: int = field(
        init=False, repr=False, default_factory=_make_jpy_instance
    )

    def __post_init__(self):
        RulesetCollection.add(self)
        self.start_session()

    def add_rule(self, rule: Rule) -> None:
        self._rules[rule.name] = rule

    def define(self):
        return self.serialized_ruleset

    def dispatch(self, serialized_result: str) -> None:
        self._dispatch(_from_json(serialized_result))

    def start_session(self) -> int:
        if self._session_id:
            return self._session_id

        self._session_id = self._api.createRuleset(self.serialized_ruleset)
        return self._session_id

    def end_session(self) -> None:
        self._api.dispose(self._session_id)

    def get_facts(self):
        result = self._api.getFacts(self._session_id)
        return json.loads(result)

    def assert_event(self, serialized_fact: str):
        return self._process_response(
            self._api.assertEvent(self._session_id, serialized_fact)
        )

    def assert_fact(self, serialized_fact: str):
        return self._process_response(
            self._api.assertFact(self._session_id, serialized_fact)
        )

    def retract_fact(self, serialized_fact: str):
        return self._process_response(
            self._api.retractFact(self._session_id, serialized_fact)
        )

    def get_pending_events(self):
        pass

    def _process_response(self, payload: str):
        results = json.loads(payload)
        for result in results:
            self._dispatch(result)

    def _dispatch(self, rule_match: dict) -> None:
        for name, value in rule_match.items():
            if name in self._rules:
                self._rules[name].callback(Matches(data=value))
            else:
                raise RuleNotFoundError(
                    "Rule " + name + " does not exist in Ruleset " + self.name
                )


@dataclass
class RulesetCollection:
    __cached_objects: ClassVar[Dict[str, Ruleset]] = {}

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
    def get_by_session_id(cls, session_id: int) -> Ruleset:
        for obj in cls.__cached_objects.values():
            if obj._session_id == session_id:
                return obj
            raise RulesetNotFoundError(
                "Ruleset with session id" + str(session_id) + " not found"
            )


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


def end_session(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).end_session()


def get_facts(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_facts()


def get_pending_events(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_pending_events()
