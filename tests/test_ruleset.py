import json
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
    rules = [
        {"condition": {"all": ["event.i == 3"]}, "action": {"debug": None}}
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event(json.dumps(dict(i=3)))
    my_callback1.assert_called_with(result)


def test_ruleset_handled_multiple_conditions_all():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result = Matches(_m={"match1": {"i": 1}, "match2": {"i": 9}}, m=None)
    rules = [
        {
            "condition": {
                "all": [
                    "events.match1 << event.i == 1",
                    "events.match2 << event.i == 9",
                ]
            },
            "action": {"debug": None},
        }
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event(json.dumps(dict(i=1)))
    rs.assert_event(json.dumps(dict(i=9)))
    my_callback1.assert_called_with(result)


def test_ruleset_handled_multiple_conditions_any():
    my_callback = mock.Mock()
    result = Matches(_m={"match1": {"i": 1}}, m=None)
    rules = [
        {
            "condition": {
                "any": [
                    "events.match1 << event.i == 1",
                    "events.match2 << event.i == 9",
                ]
            },
            "action": {"debug": None},
        }
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("r_0", my_callback)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event(json.dumps(dict(i=1)))
    my_callback.assert_called_with(result)


def test_ruleset_handled_multiple_rules():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result1 = Matches(_m={"match1": {"i": 1}}, m=None)
    result2 = Matches(_m={"match2": {"i": 1}}, m=None)

    rules = [
        {
            "name": "rule1",
            "condition": {"all": ["events.match1 << event.i == 1"]},
            "action": {"debug": None},
        },
        {
            "name": "rule2",
            "condition": {"all": ["events.match2 << event.i == 1"]},
            "action": {"debug": None},
        },
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("rule1", my_callback1), Rule("rule2", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event(json.dumps(dict(i=1)))
    my_callback1.assert_called_with(result1)
    my_callback2.assert_called_with(result2)


def test_ruleset_assert_fact():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result1 = Matches(_m={"match1": {"f": 9}}, m=None)
    result2 = Matches(_m={"match2": {"f": 9}}, m=None)

    rules = [
        {
            "name": "rule1",
            "condition": {"all": ["facts.match1 << fact.f == 9"]},
            "action": {"debug": None},
        },
        {
            "name": "rule2",
            "condition": {"all": ["facts.match2 << fact.f == 9"]},
            "action": {"debug": None},
        },
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(f=8),
    )
    for r in [Rule("rule1", my_callback1), Rule("rule2", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_fact(json.dumps(dict(f=9)))
    my_callback1.assert_called_with(result1)
    my_callback2.assert_called_with(result2)


def test_ruleset_retract_fact():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result1 = Matches(_m={"match1": {"f": 9}}, m=None)
    result2 = Matches(_m={"match2": {"f": 9}}, m=None)

    rules = [
        {
            "name": "rule1",
            "condition": {"all": ["facts.match1 << fact.f == 9"]},
            "action": {"debug": None},
        },
        {
            "name": "rule2",
            "condition": {"all": ["facts.match2 << fact.f == 9"]},
            "action": {"debug": None},
        },
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(f=8, a=9),
    )
    for r in [Rule("rule1", my_callback1), Rule("rule2", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.retract_fact(json.dumps(dict(f=9)))
    my_callback1.assert_called_with(result1)
    my_callback2.assert_called_with(result2)

    response = rs.get_facts()
    assert "f" not in response.keys()
    assert "a" in response.keys()


def test_ruleset_handled_multiple_rules_without_assignment():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result1 = Matches(_m={"m": {"i": 1}}, m=SingleMatchData(_d={"i": 1}))
    result2 = Matches(_m={"m": {"i": 1}}, m=SingleMatchData(_d={"i": 1}))

    rules = [
        {
            "name": "rule1",
            "condition": {"all": ["event.i == 1"]},
            "action": {"debug": None},
        },
        {
            "name": "rule2",
            "condition": {"all": ["event.i == 1"]},
            "action": {"debug": None},
        },
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("rule1", my_callback1), Rule("rule2", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()
    rs.assert_event(json.dumps(dict(i=1)))
    my_callback1.assert_called_with(result1)
    my_callback2.assert_called_with(result2)


def test_ruleset_collection():
    rs = Ruleset(name="b", serialized_ruleset=json.dumps([]), context={})
    assert RulesetCollection.get("b") == rs


def test_ruleset_collection_missing_object():
    with pytest.raises(drools.exceptions.RulesetNotFoundError):
        RulesetCollection.get("non_existent_object")


def test_ruleset_handled_msg_via_collection():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    result = Matches(_m={"m": {"i": 3}}, m=SingleMatchData(_d={"i": 3}))
    rules = [
        {"condition": {"all": ["event.i == 3"]}, "action": {"debug": None}}
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()

    assert_event("my_ruleset", json.dumps(dict(i=3)))
    my_callback1.assert_called_with(result)


def test_ruleset_skipped_via_collection():
    my_callback1 = mock.Mock()
    my_callback2 = mock.Mock()
    rules = [
        {"condition": {"all": ["event.i == 3"]}, "action": {"debug": None}}
    ]

    rs = Ruleset(
        name="my_ruleset",
        serialized_ruleset=json.dumps(rules),
        context=dict(fact=dict(arch="x64")),
    )
    for r in [Rule("r_0", my_callback1), Rule("r_1", my_callback2)]:
        rs.add_rule(r)

    rs.start_session()

    assert_event("my_ruleset", json.dumps(dict(i=9)))
    assert (
        not my_callback1.called
    ), "my_callback1 was called and should not have been"
