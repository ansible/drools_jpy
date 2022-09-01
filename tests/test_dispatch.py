from unittest import mock

import pytest

from drools.dispatch import Dispatch
from drools.exceptions import RuleNotFoundError, RulesetNotFoundError
from drools.rule import Rule
from drools.ruleset import Matches, Ruleset, SingleMatchData


def test_run():
    my_callback = mock.Mock()

    result = Matches(_m={"m": {"i": 3}}, m=SingleMatchData(_d={"i": 3}))
    dr_rules = '{"fred": {"all": [{"m": {"i": 3}}]}}'
    rs = Ruleset(name="Ruleset1", serialized_ruleset=dr_rules)
    rs.add_rule(Rule("fred", my_callback))
    session_id = rs.start_session()
    serialized_result = '{"fred": {"m": {"i": 3}}}'
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
    my_callback = mock.Mock()

    dr_rules = '{"fred": {"all": [{"m": {"i": 3}}]}}'
    rs = Ruleset(name="Ruleset1", serialized_ruleset=dr_rules)
    rs.add_rule(Rule("fred", my_callback))
    session_id = rs.start_session()
    serialized_result = '{"wilma": {"m": {"i": 3}}}'
    dispatch = Dispatch(
        session_id=session_id, serialized_result=serialized_result
    )
    with pytest.raises(RuleNotFoundError):
        dispatch.run()
