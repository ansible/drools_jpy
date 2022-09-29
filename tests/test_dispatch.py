import json
import os
from unittest import mock

import pytest
import yaml

from drools.dispatch import Dispatch
from drools.exceptions import RuleNotFoundError, RulesetNotFoundError
from drools.rule import Rule
from drools.ruleset import Matches, Ruleset


def load_ast(filename: str) -> dict:
    test_dir = os.path.dirname(os.path.realpath(__file__))
    with open(f"{test_dir}/{filename}") as f:
        test_data = yaml.safe_load(f)
    return test_data


def test_run():
    test_data = load_ast("asts/rules_with_assignment.yml")

    my_callback = mock.Mock()
    result = Matches(data={"first": {"i": 67}})

    ruleset_data = test_data[0]["RuleSet"]
    rs = Ruleset(
        name=ruleset_data["name"], serialized_ruleset=json.dumps(ruleset_data)
    )
    rs.add_rule(Rule("assignment", my_callback))

    session_id = rs.start_session()
    serialized_result = '{"assignment": {"first": {"i": 67}}}'
    dispatch = Dispatch(
        session_id=session_id, serialized_result=serialized_result
    )
    dispatch.run()
    my_callback.assert_called_with(result)


def test_missing_session_id():
    dispatch = Dispatch(session_id=12345678, serialized_result="abc")
    with pytest.raises(RulesetNotFoundError):
        dispatch.run()


def test_missing_rule():
    test_data = load_ast("asts/rules_with_assignment.yml")

    my_callback = mock.Mock()

    ruleset_data = test_data[0]["RuleSet"]
    rs = Ruleset(
        name=ruleset_data["name"], serialized_ruleset=json.dumps(ruleset_data)
    )
    rs.add_rule(Rule("assignment", my_callback))

    session_id = rs.start_session()
    serialized_result = '{"nada": {"first": {"i": 67}}}'
    dispatch = Dispatch(
        session_id=session_id, serialized_result=serialized_result
    )
    with pytest.raises(RuleNotFoundError):
        dispatch.run()
