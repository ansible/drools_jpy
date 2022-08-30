from unittest import mock

import pytest

import drools
from drools.rule import Rule
from drools.ruleset import (
    Matches,
    Ruleset,
    RulesetCollection,
    SingleMatchData,
    assert_event,
)


def test_ruleset_handled_msg():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result = Matches(_m={"m": {"i": 3}}, m=SingleMatchData(_d={"i": 3}))

    rs = Ruleset("my_ruleset", '{"r_0": {"all": [{"m": {"i": 3}}]}}')
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event("{i: 3}")
    my_callback1.assert_called_with(result)


def test_ruleset_collection():
    rs = Ruleset("b", "2")
    assert RulesetCollection.get("b") == rs


def test_ruleset_collection_missing_object():
    with pytest.raises(drools.exceptions.RulesetNotFoundError):
        RulesetCollection.get("non_existent_object")


def test_ruleset_handled_msg_via_collection():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result = Matches(_m={"m": {"i": 3}}, m=SingleMatchData(_d={"i": 3}))

    rs = Ruleset("my_ruleset", '{"r_0": {"all": [{"m": {"i": 3}}]}}')
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()

    assert_event("my_ruleset", "{i: 3}")
    my_callback1.assert_called_with(result)
