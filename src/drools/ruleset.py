import errno
import glob
import json
import logging
import os
from dataclasses import dataclass, field
from typing import ClassVar, Dict, List

import jpyutil

from .exceptions import RuleNotFoundError, RulesetNotFoundError
from .rule import Rule

DEFAULT_DROOLS_CLASS = (
    "org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine"
)

DROOLS_JPY_GC_AFTER = int(os.environ.get("DROOLS_JPY_GC_AFTER", 1000))

logger = logging.getLogger(__name__)


def _get_jar() -> str:
    package_dir = os.path.dirname(os.path.realpath(__file__))
    jars = glob.glob(os.path.join(package_dir, "jars", "*.jar"))
    if len(jars) == 0:
        raise FileNotFoundError(errno.ENOENT, "No jars found", package_dir)
    return jars[0]


def _make_jpy_instance():
    jar_file_path = os.environ.get("DROOLS_JPY_CLASSPATH", _get_jar())
    if not os.path.exists(jar_file_path):
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), jar_file_path
        )
    logger.info("Using jar: %s", jar_file_path)
    max_mem = os.environ.get("DROOLS_JPY_JVM_MAXMEM", "512M")

    jvm_options = []

    # parse debug port option
    debug_port = (
        os.environ.get("DROOLS_JPY_JVM_DEBUG", "False").lower().strip()
    )

    if debug_port:
        if "true" == debug_port:
            debug_port = 5005  # default debug port for JVM
        else:
            try:
                debug_port = int(debug_port)
            except ValueError:
                debug_port = False

        if debug_port:
            jvm_options.append(
                "-agentlib:jdwp=transport=dt_socket,"
                "server=y,suspend=y,address=*:" + str(debug_port)
            )

    # parse log config options
    # assumes the options listed here:
    # https://www.slf4j.org/api/org/slf4j/simple/SimpleLogger.html
    # without the `org.slf4j.simpleLogger.` prefix; e.g.:
    #      defaultLogLevel=trace
    # stands for:
    #       org.slf4j.simpleLogger.defaultLogLevel=trace
    # comma separated list. e.g.:
    #       defaultLogLevel=trace,logFile=/file/path
    # stands for:
    #       -Dorg.slf4j.simpleLogger.defaultLogLevel=trace
    #       -Dorg.slf4j.simpleLogger.logFile=/file/path

    common = "showDateTime=true,dateTimeFormat=yyyy-MM-dd HH:mm:ss SSS"
    if logging.DEBUG >= logging.root.level:
        default_log_str = "logFile=System.out,defaultLogLevel=debug," + common
    elif logging.INFO >= logging.root.level:
        default_log_str = "logFile=System.out,defaultLogLevel=info," + common
    else:
        default_log_str = "logFile=System.err,defaultLogLevel=error," + common

    log_options = (
        os.environ.get("DROOLS_JPY_JVM_LOG", default_log_str)
    ).split(
        ","
    )  # split on comma
    jvm_log_options = [
        "-Dorg.slf4j.simpleLogger." + kv for kv in log_options
    ]  # add prefix

    jvm_options.extend(jvm_log_options)

    log_delay = os.environ.get("DROOLS_LOG_DELAY", "5")
    jvm_options.append("-Ddrools.delay.warning.threshold=" + log_delay)

    jpyutil.init_jvm(
        jvm_maxmem=max_mem,
        jvm_classpath=[jar_file_path],
        jvm_options=jvm_options,
    )

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

    def __post_init__(self):
        self._api = RulesetCollection.api()
        self.start_session()
        RulesetCollection.add(self)

    def add_rule(self, rule: Rule) -> None:
        self._rules[rule.name] = rule

    def define(self):
        return self.serialized_ruleset

    def dispatch(self, serialized_result: str) -> None:
        self._dispatch(_from_json(serialized_result))

    def start_session(self) -> int:
        if self._session_id:
            return self._session_id
        logger.debug("Creating Drools Ruleset")
        self._session_id = self._api.createRuleset(self.serialized_ruleset)
        logger.debug("Ruleset Session ID : " + str(self._session_id))
        return self._session_id

    def end_session(self) -> Dict:
        result = self._api.dispose(self._session_id)
        if result:
            return json.loads(result)
        return {}

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

    def retract_matching_facts(
        self, serialized_fact: str, partial: bool, exclude_keys: List[str]
    ):
        return self._process_response(
            self._api.retractMatchingFacts(
                self._session_id, serialized_fact, partial, exclude_keys
            )
        )

    def session_stats(self) -> Dict:
        result = self._api.sessionStats(self._session_id)
        if result:
            return json.loads(result)
        return {}

    def advance_time(self, amount: int, units: str):
        return self._api.advanceTime(self._session_id, amount, units)

    def get_pending_events(self):
        pass

    def _process_response(self, payload: str):
        if payload is None:
            return

        results = json.loads(payload)
        for result in results:
            self._dispatch(result)

    def _dispatch(self, rule_match: dict) -> None:
        for name, value in rule_match.items():
            if name in self._rules:
                logger.debug(
                    "Calling rule : "
                    + name
                    + " in session: "
                    + str(self._session_id)
                )
                self._rules[name].callback(Matches(data=value))
            else:
                raise RuleNotFoundError(
                    "Rule " + name + " does not exist in Ruleset " + self.name
                )


@dataclass
class RulesetCollection:
    __cached_objects: ClassVar[Dict[str, Ruleset]] = {}
    engine = None

    @classmethod
    def api(cls):
        cls.create_engine()
        return cls.engine

    @classmethod
    def create_engine(cls):
        if not cls.engine:
            cls.engine = _make_jpy_instance()

    @classmethod
    def response_port(cls):
        cls.create_engine()
        return cls.engine.port()

    @classmethod
    def shutdown(cls):
        cls.engine.shutdown()
        cls.engine = None

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
            "Ruleset with session id " + str(session_id) + " not found"
        )


message_counter = 0
java_lang_System = None


def call_garbage_collector():
    global message_counter, java_lang_System
    if DROOLS_JPY_GC_AFTER > 0 and message_counter > DROOLS_JPY_GC_AFTER:
        if java_lang_System is None:
            import jpy

            java_lang_System = jpy.get_type("java.lang.System")
        java_lang_System.gc()
        message_counter = 0
    else:
        message_counter += 1


def post(ruleset_name: str, serialized_event: str):
    call_garbage_collector()
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


def retract_matching_facts(
    ruleset_name: str,
    serialized_fact: str,
    partial: bool,
    exclude_keys: List[str],
):
    return RulesetCollection.get(ruleset_name).retract_matching_facts(
        _to_json(serialized_fact), partial, exclude_keys
    )


def end_session(ruleset_name: str) -> Dict:
    return RulesetCollection.get(ruleset_name).end_session()


def session_stats(ruleset_name: str) -> Dict:
    return RulesetCollection.get(ruleset_name).session_stats()


def get_facts(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_facts()


def get_pending_events(ruleset_name: str):
    return RulesetCollection.get(ruleset_name).get_pending_events()


def advance_time(ruleset_name: str, amount: int, units: str):
    return RulesetCollection.get(ruleset_name).advance_time(amount, units)
