import json

from drools import durable_rules_engine


def test_create_ruleset():
    session_id = durable_rules_engine.create_ruleset(
        "abc", '{"r_0": {"all": [{"m": {"i": 3}}]}}'
    )

    for i in range(4):
        result = durable_rules_engine.assert_event(
            session_id, json.dumps(dict(i=i))
        )
        result = durable_rules_engine.start_action_for_state(
            session_id, session_id
        )
    durable_rules_engine.complete_and_start_action(session_id, session_id)
    rule = json.loads(result[1])
    assert rule["r_0"] == dict(m=dict(i=3))
