from unittest import mock

import pytest

import drools
from drools.rule import Rule
from drools.ruleset import Ruleset, RulesetCollection


def test_ruleset_unhandled_msg():
    my_callback = mock.Mock()

    rs = Ruleset("my_ruleset", '{"r_0": {"all": [{"m": {"i": 3}}]}}')
    for r in [Rule("fred", my_callback), Rule("barney", my_callback)]:
        rs.add_rule(r)

    rs.create_session()
    with pytest.raises(drools.exceptions.MessageNotHandledError):
        rs.assert_event("{i: 1}")


def test_ruleset_handled_msg():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()

    rs = Ruleset("my_ruleset", '{"r_0": {"all": [{"m": {"i": 3}}]}}')
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.create_session()
    rs.assert_event("{i: 3}")
    my_callback1.assert_called_with(dict(m=dict(i=3)))


def test_ruleset_collection():
    rs = Ruleset("b", "2")
    assert RulesetCollection.get("b") == rs


def test_ruleset_collection_missing_object():
    with pytest.raises(drools.exceptions.RulesetNotFoundError):
        RulesetCollection.get("non_existent_object")


def test_ruleset_handled_msg_via_collection():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()

    rs = Ruleset("my_ruleset", '{"r_0": {"all": [{"m": {"i": 3}}]}}')
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.create_session()

    RulesetCollection.assert_event("my_ruleset", "{i: 3}")
    my_callback1.assert_called_with(dict(m=dict(i=3)))
