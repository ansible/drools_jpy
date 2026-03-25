import asyncio
import json
import os
import secrets
import shutil
import uuid

import pytest
from ha_test_utils import (
    assert_rule_fired,
    assert_rule_not_fired,
    cancel_async_task,
    create_ruleset_with_callback,
    fire_rule_and_get_matching_uuid,
    generate_encryption_key,
    ha_worker_manager,
    load_ast,
    query_raw_column,
    verify_grace_period_recovery,
    wait_for_async_processing,
)

from drools.ruleset import (
    RulesetCollection,
    action_info_exists,
    add_action_info,
    delete_action_info,
    enable_leader,
    get_action_info,
    get_action_status,
    get_ha_stats,
    get_partial_event_ids,
    shutdown,
    update_action_info,
)


def _generate_unique_h2_file_path():
    """Generate a unique H2 file path using UUID to avoid lock
    contention between tests."""
    return f"./target/h2-test-{uuid.uuid4()}/eda_ha"


@pytest.fixture
def db_params():
    """DB connection parameters for testing"""
    generated_h2_dir = None
    env_h2_file = os.environ.get("DROOLS_HA_H2_FILE")
    if env_h2_file:
        h2_file_path = env_h2_file
    else:
        h2_file_path = _generate_unique_h2_file_path()
        generated_h2_dir = os.path.dirname(h2_file_path)
    params = {
        "db_type": os.environ.get(
            "DROOLS_HA_DB_TYPE", "h2"
        ),  # "h2" or "postgres"
        "db_file_path": h2_file_path,
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "eda_ha_db"),
        "user": os.environ.get("POSTGRES_USER", "eda_user"),
        "password": os.environ.get("POSTGRES_PASSWORD", secrets.token_hex(8)),
    }
    yield params
    # Clean up the generated H2 directory after the test
    if generated_h2_dir and os.path.isdir(generated_h2_dir):
        shutil.rmtree(generated_h2_dir, ignore_errors=True)


@pytest.fixture
def ha_config():
    """HA configuration parameters"""
    return {
        "write_after": 1,  # not yet implemented
        "dedup_buffer_size": 5,  # default is 5
    }


@pytest.mark.asyncio
async def test_ha_initialization_and_leader_lifecycle(db_params, ha_config):
    """Test HA initialization, createRuleset, enableLeader, and assertEvent"""

    instance_uuid = str(uuid.uuid4())
    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    async with ha_worker_manager(
        instance_uuid,
        "worker-1",
        db_params,
        ha_config,
        ruleset_data,
        "assignment",
        captured_matches,
    ) as ctx:
        rs = ctx["ruleset"]
        my_callback = ctx["callback"]
        async_task = ctx["async_task"]

        # Verify HA stats after enabling leader
        ha_stats = get_ha_stats()
        print(f"HA Stats after enabling leader: {ha_stats}")
        assert ha_stats is not None
        assert isinstance(ha_stats, dict)

        # Assert an event that will match the rule
        rs.assert_event(json.dumps({"i": 67}))

        ha_stats = get_ha_stats()
        print(f"HA Stats after sending an event: {ha_stats}")

        # Wait for async task to complete
        try:
            await cancel_async_task(async_task)
        except asyncio.CancelledError:  # NOSONAR
            pass  # Expected when cancelling

        # Verify the callback was called
        assert_rule_fired(my_callback, captured_matches)

        # Extract matching_uuid from the Matches object
        matching_uuid = captured_matches[0].matching_uuid

        # Test ActionInfo operations if matching_uuid is available
        if matching_uuid:
            action_data = json.dumps({"action": "print_event", "status": "1"})

            # Test adding action info
            add_action_info(
                ruleset_data["name"], matching_uuid, 0, action_data
            )

            # Verify action exists
            assert action_info_exists(ruleset_data["name"], matching_uuid, 0)

            # Get action info
            retrieved_action = get_action_info(
                ruleset_data["name"], matching_uuid, 0
            )
            assert retrieved_action == action_data

            # Clean up action info
            delete_action_info(ruleset_data["name"], matching_uuid)


@pytest.mark.asyncio
async def test_action_info_lifecycle(db_params):
    """Test ActionInfo CRUD operations"""

    instance_uuid = str(uuid.uuid4())
    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    async with ha_worker_manager(
        instance_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        "assignment",
        captured_matches,
    ) as ctx:
        rs = ctx["ruleset"]
        my_callback = ctx["callback"]
        async_task = ctx["async_task"]

        # Fire rule and get matching_uuid
        matching_uuid = await fire_rule_and_get_matching_uuid(
            rs, my_callback, async_task, captured_matches
        )

        # Test ActionInfo operations
        action_index = 0
        action_data = json.dumps({"action": "print_event", "status": "1"})

        # 1. Check action doesn't exist initially
        assert not action_info_exists(
            ruleset_data["name"], matching_uuid, action_index
        )

        # 2. Add action info
        add_action_info(
            ruleset_data["name"], matching_uuid, action_index, action_data
        )

        # 3. Verify action exists
        assert action_info_exists(
            ruleset_data["name"], matching_uuid, action_index
        )

        # 4. Get action info
        retrieved_action = get_action_info(
            ruleset_data["name"], matching_uuid, action_index
        )
        assert retrieved_action == action_data

        # 5. Get action status
        status = get_action_status(
            ruleset_data["name"], matching_uuid, action_index
        )
        assert status is not None

        # 6. Delete action info
        delete_action_info(ruleset_data["name"], matching_uuid)

        # 7. Verify action no longer exists
        assert not action_info_exists(
            ruleset_data["name"], matching_uuid, action_index
        )


@pytest.mark.asyncio
async def test_ha_stats(db_params):
    """Test getting HA statistics"""

    instance_uuid = str(uuid.uuid4())
    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    async with ha_worker_manager(
        instance_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        "assignment",
        captured_matches,
        auto_enable_leader=False,
    ):
        # Get stats before enabling leader
        stats_before = get_ha_stats()
        print(f"HA Stats before enabling leader: {stats_before}")
        assert isinstance(stats_before, dict)

        # Enable leader
        enable_leader()

        # Get stats after enabling leader
        stats_after = get_ha_stats()
        print(f"HA Stats after enabling leader: {stats_after}")
        assert isinstance(stats_after, dict)


@pytest.mark.asyncio
async def test_multiple_action_infos(db_params):
    """Test managing multiple actions for the same matching event"""

    instance_uuid = str(uuid.uuid4())
    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    async with ha_worker_manager(
        instance_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        "assignment",
        captured_matches,
    ) as ctx:
        rs = ctx["ruleset"]
        my_callback = ctx["callback"]
        async_task = ctx["async_task"]

        # Fire rule and get matching_uuid
        matching_uuid = await fire_rule_and_get_matching_uuid(
            rs, my_callback, async_task, captured_matches
        )

        # Add multiple actions
        for i in range(3):
            action_data = json.dumps({"action": f"action_{i}", "index": i})
            add_action_info(
                ruleset_data["name"], matching_uuid, i, action_data
            )

        # Verify all actions exist
        for i in range(3):
            assert action_info_exists(ruleset_data["name"], matching_uuid, i)
            action = get_action_info(ruleset_data["name"], matching_uuid, i)
            assert f"action_{i}" in action

        # Delete all actions at once
        delete_action_info(ruleset_data["name"], matching_uuid)

        # Verify all are deleted
        for i in range(3):
            assert not action_info_exists(
                ruleset_data["name"], matching_uuid, i
            )


@pytest.mark.asyncio
async def test_ha_failover_scenario(db_params):
    """Test HA failover scenario with leader switch"""

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    # Leader 1 starts
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        "assignment",
        captured_matches,
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        async_task1 = ctx["async_task"]

        print("worker-1 became a Leader")

        # Leader 1 asserts an event to get a real matching_uuid
        rs1.assert_event(json.dumps({"i": 67}))

        # Wait for match
        try:
            await cancel_async_task(async_task1)

        except asyncio.CancelledError:  # NOSONAR
            pass  # Expected when cancelling

        assert len(captured_matches) > 0
        matching_uuid = captured_matches[0].matching_uuid
        assert matching_uuid is not None
        print(f"Matching UUID: {matching_uuid}")

        # Leader 1 creates action info
        add_action_info(
            ruleset_data["name"],
            matching_uuid,
            0,
            json.dumps({"action": "test", "status": "1"}),
        )
        print("Leader worker-1 added action info")

        stats_before_failover = get_ha_stats()
        print(f"HA Stats before failover: {stats_before_failover}")

    print("RulesetCollection shut down")

    # Leader 2 starts with the SAME HA UUID
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        "assignment",
        [],
        auto_enable_leader=False,
    ):
        # Yield to ensure async handler starts reading before enableLeader
        print("Yielding to async handler before enabling leader...")
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for potential recovery message
        print("Waiting for recovery message...")
        await wait_for_async_processing(0.5)

        # Leader 2 should be able to read the action info created by Leader 1
        assert action_info_exists(ruleset_data["name"], matching_uuid, 0)
        print("Leader worker-2 successfully read action info from worker-1")

        stats_after_failover = get_ha_stats()
        print(f"HA Stats after failover: {stats_after_failover}")

        # Clean up
        delete_action_info(ruleset_data["name"], matching_uuid)
        print("Leader worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_failover_with_partial_match(db_params):
    """Test HA failover: partial match completed by worker-2.

    Uses rules_with_multiple_conditions_all_assignment which has an
    AllCondition requiring two events: first (i==0) and second (i==1).
    Worker-1 sends only the first event, then goes down. Worker-2 takes
    over and sends the second event, which should fire the rule thanks
    to HA state recovery.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast(
        "asts/rules_with_multiple_conditions_all_assignment.yml"
    )
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "multiple conditions"

    # --- Worker 1: send partial match (first condition only) ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send first event (i==0) — matches only the first condition
        rs1.assert_event(json.dumps({"i": 0}))
        print("worker-1 sent event {i: 0} (partial match)")

        # Give some time for async processing
        await wait_for_async_processing(0.5)

        # Rule should not have fired (only one of two conditions met)
        assert_rule_not_fired(
            my_callback1,
            [],
            "Rule should not fire with only one condition matched",
        )
        print("Confirmed: rule did not fire on worker-1 (partial match)")

    print("RulesetCollection shut down")

    # --- Worker 2: takes over and completes the match ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # Yield to ensure async handler starts reading before enableLeader
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of the partial match from worker-1
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # Send second event (i==1) — completes the AllCondition,
        # rule should fire
        rs2.assert_event(json.dumps({"i": 1}))
        print("worker-2 sent event {i: 1} (completing the match)")

        # Verify the rule fired on worker-2
        assert_rule_fired(
            my_callback2,
            captured_matches,
            "Rule should fire after both conditions are met",
        )
        matching_uuid = captured_matches[0].matching_uuid
        print(f"Rule fired on worker-2! matching_uuid: {matching_uuid}")

        # Verify match data contains both events
        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Clean up while engine is still alive
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_failover_with_multiple_actions(db_params):
    """Test HA failover with multiple action infos across workers.

    Uses test_multiple_actions_ast.yml which has a rule with 3 actions.
    Worker-1 fires the rule, adds 3 action infos (status=1), updates one
    to status=3, then goes down. Worker-2 takes over via failover, receives
    MatchingEvent recovery, updates the remaining 2 action infos to status=3,
    then deletes all action info for the matching event.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_multiple_actions_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "r1"
    captured_matches = []

    # --- Worker 1: fire rule, add action infos, update one, then go down ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # 1. Send event to fire the rule (i==1)
        rs1.assert_event(json.dumps({"i": 1}))
        print("worker-1 sent event {i: 1}")

        # Verify the rule fired
        assert_rule_fired(
            my_callback1, captured_matches, "Rule should fire when i==1"
        )

        # 2. Extract matching_uuid
        matching_uuid = captured_matches[0].matching_uuid
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"
        print(f"worker-1 received match, matching_uuid: {matching_uuid}")

        # 3. Add 3 action infos with status=1
        for i in range(3):
            action_data = json.dumps({"action": f"action_{i}", "status": "1"})
            add_action_info(
                ruleset_data["name"], matching_uuid, i, action_data
            )
        print("worker-1 added 3 action infos with status=1")

        # Verify all 3 action infos exist
        for i in range(3):
            assert action_info_exists(ruleset_data["name"], matching_uuid, i)

        # 4. Update action 0 to status=3 (simulating action completed)
        update_action_info(
            ruleset_data["name"],
            matching_uuid,
            0,
            json.dumps({"action": "action_0", "status": "3"}),
        )
        print("worker-1 updated action 0 to status=3")

        # Verify action 0 is updated
        action_0 = json.loads(
            get_action_info(ruleset_data["name"], matching_uuid, 0)
        )
        assert action_0["status"] == "3"

        print("worker-1 disabled and session ended")

    print("RulesetCollection shut down")

    # --- Worker 2: takes over, receives recovery, updates
    # remaining actions ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        auto_enable_leader=False,
    ) as ctx:
        # Yield to ensure async handler starts reading before enableLeader
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # 6. Wait for MatchingEvent recovery
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # 7. Verify all 3 action infos from worker-1 are still available
        for i in range(3):
            assert action_info_exists(
                ruleset_data["name"], matching_uuid, i
            ), f"action {i} should survive failover"
        print("worker-2 verified all 3 action infos survived failover")

        # Verify action 0 still has status=3 from worker-1's update
        action_0 = json.loads(
            get_action_info(ruleset_data["name"], matching_uuid, 0)
        )
        assert (
            action_0["status"] == "3"
        ), "action 0 status should be 3 from worker-1"

        # Verify actions 1 and 2 still have status=1
        for i in [1, 2]:
            action_i = json.loads(
                get_action_info(ruleset_data["name"], matching_uuid, i)
            )
            assert (
                action_i["status"] == "1"
            ), f"action {i} status should still be 1"

        # 8. Update remaining actions (1, 2) to status=3
        for i in [1, 2]:
            update_action_info(
                ruleset_data["name"],
                matching_uuid,
                i,
                json.dumps({"action": f"action_{i}", "status": "3"}),
            )
        print("worker-2 updated actions 1 and 2 to status=3")

        # Verify all actions now have status=3
        for i in range(3):
            action_i = json.loads(
                get_action_info(ruleset_data["name"], matching_uuid, i)
            )
            assert (
                action_i["status"] == "3"
            ), f"action {i} should have status=3"

        # 9. Delete all action info for the matching event
        delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 deleted all action info")

        # Verify all actions are gone
        for i in range(3):
            assert not action_info_exists(
                ruleset_data["name"], matching_uuid, i
            ), f"action {i} should be deleted"

        print("worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_failover_once_after(db_params):
    """Test OnceAfter rule surviving a failover.

    Worker-1 sends several events (building up the once_after buffer)
    then goes down before the time window expires. Worker-2 takes over,
    recovers the buffered state, advances time past the window, and
    verifies the rule fires with accumulated events from worker-1.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_once_after_ha_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "r1"

    # --- Worker 1: send events, go down before once_after fires ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send 5 events — once_after window is 2 seconds
        for i in range(5):
            rs1.assert_event(json.dumps({"i": i, "meta": {"host": "A"}}))
        print("worker-1 sent 5 events")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Rule should NOT have fired (still within once_after window)
        assert_rule_not_fired(
            my_callback1,
            [],
            "Rule should not fire before once_after window expires",
        )
        print("Confirmed: rule did not fire on worker-1")

    print("RulesetCollection shut down")

    # --- Worker 2: take over, recover state, advance time ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of buffered state from worker-1
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # Advance time past the once_after window (2 seconds)
        rs2.advance_time(3, "Seconds")
        print("worker-2 advanced time by 3 seconds")

        # Wait for async match delivery
        await wait_for_async_processing(1.0)

        # The rule should have fired after time advanced past window
        assert_rule_fired(
            my_callback2,
            captured_matches,
            "Rule should fire after once_after window expires",
        )
        matching_uuid = captured_matches[0].matching_uuid
        print(f"Rule fired on worker-2! matching_uuid: {matching_uuid}")

        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_duplicate_event_detection_survives_failover(
    db_params, ha_config
):
    """Test duplicate event detection surviving a failover.

    Worker-1 processes an event with a specific UUID (temperature > 30),
    then goes down. Worker-2 takes over and re-sends the same event
    (same UUID). The duplicate should be detected and ignored (no match).
    A new event with a different UUID should still be processed normally.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_dedup_ha_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "temperature_alert"

    dup_event_uuid = "failover-1111-2222-3333-444444444444"
    new_event_uuid = "new-event-2222-3333-4444-555555555555"
    captured_matches_1 = []

    # --- Worker 1: process event, then go down ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        ha_config,
        ruleset_data,
        rule_name,
        captured_matches_1,
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send event with temperature=35 (> 30) and a specific UUID
        event = json.dumps(
            {
                "meta": {"uuid": dup_event_uuid},
                "temperature": 35,
            }
        )
        rs1.assert_event(event)
        print("worker-1 sent event with temperature=35")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Rule should have fired (temperature 35 > 30)
        assert_rule_fired(
            my_callback1,
            captured_matches_1,
            "Rule should fire for temperature > 30",
        )
        matching_uuid = captured_matches_1[0].matching_uuid
        print(f"Rule fired on worker-1! matching_uuid: {matching_uuid}")

    print("RulesetCollection shut down")

    # --- Worker 2: take over, re-send same event (should be ignored) ---
    captured_matches_2 = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches_2,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery (may dispatch recovery events)
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # Reset tracking after recovery so recovery events don't interfere
        my_callback2.reset_mock()
        captured_matches_2.clear()
        print("Reset mock after recovery")

        # Re-send the same event (same UUID) — should be detected
        # as duplicate and ignored
        event = json.dumps(
            {
                "meta": {"uuid": dup_event_uuid},
                "temperature": 35,
            }
        )
        rs2.assert_event(event)
        print("worker-2 re-sent duplicate event (same UUID as worker-1)")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Rule should NOT fire — event is a duplicate
        assert_rule_not_fired(
            my_callback2,
            captured_matches_2,
            "Duplicate event should be ignored after failover",
        )
        print("Confirmed: duplicate event was ignored on worker-2")

        # Send a NEW event with a different UUID — should be processed normally
        new_event = json.dumps(
            {
                "meta": {"uuid": new_event_uuid},
                "temperature": 40,
            }
        )
        rs2.assert_event(new_event)
        print("worker-2 sent new event with temperature=40")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Rule should fire for the new event
        assert_rule_fired(
            my_callback2,
            captured_matches_2,
            "New event should be processed normally",
        )
        matching_uuid = captured_matches_2[0].matching_uuid
        print(
            f"Rule fired on worker-2 for new event! "
            f"matching_uuid: {matching_uuid}"
        )

        # Clean up
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_failover_timed_out(db_params):
    """Test NotAllCondition (TimedOut) rule surviving a failover.

    Uses a NotAllCondition rule that expects events i and j within
    2 seconds. Worker-1 sends only event i, then goes down before
    the timeout. Worker-2 takes over, recovers the partial match,
    advances time past the timeout, and the rule fires because
    event j never arrived.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_not_all_ha_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "r1"

    # --- Worker 1: send event i, go down before timeout ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send event i — starts the NotAllCondition timer
        rs1.assert_event(json.dumps({"i": 42, "host": "A"}))
        print("worker-1 sent event {i: 42}")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Rule should NOT have fired yet (timeout hasn't expired
        # and j could still arrive)
        assert_rule_not_fired(
            my_callback1, [], "Rule should not fire before timeout expires"
        )
        print("Confirmed: rule did not fire on worker-1")

    print("RulesetCollection shut down")

    # --- Worker 2: take over, recover state, advance time ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of partial match from worker-1
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # Advance time past the NotAllCondition timeout
        rs2.advance_time(3, "Seconds")
        print("worker-2 advanced time by 3 seconds")

        # Wait for async match delivery
        await wait_for_async_processing(1.0)

        # The rule should fire: event i arrived but j never did
        assert_rule_fired(
            my_callback2,
            captured_matches,
            "Rule should fire after timeout (j never arrived)",
        )
        matching_uuid = captured_matches[0].matching_uuid
        print(f"Rule fired on worker-2! matching_uuid: {matching_uuid}")

        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Verify match contains event i from worker-1
        assert "m_0" in match_data
        assert match_data["m_0"]["i"] == 42

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 cleaned up")


@pytest.mark.asyncio
async def test_ha_once_after_grace_period(db_params):
    """Test OnceAfter with grace period surviving a failover.

    Event at T=0, window=10s, crash at T=8s, Node2 clock at
    T=15s (5s past expiry). Grace=600s. The match should be
    captured and dispatched as a recovery event.
    """

    ha_uuid = str(uuid.uuid4())
    ha_config = {"expired_window_grace_period": 600}
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_once_after_grace_period_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "alert_throttle"

    # --- Worker 1: send event, advance to T=8s, go down ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        ha_config,
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send event at T=0
        rs1.assert_event(
            json.dumps({"alert": {"type": "warning", "host": "h1"}})
        )
        print("worker-1 sent alert event")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Advance to T=8s (still within 10s window)
        rs1.advance_time(8, "Seconds")
        print("worker-1 advanced time by 8 seconds")

        # Rule should NOT have fired yet
        assert_rule_not_fired(
            my_callback1,
            [],
            "Rule should not fire before once_after window expires",
        )
        print("Confirmed: rule did not fire on worker-1")

    print("RulesetCollection shut down")

    # --- Worker 2: advance to T=15s, then take over ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        ha_config,
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # The once_after timer expires during clock jump
        # (5s past expiry, within 600s grace)
        await verify_grace_period_recovery(
            rs2, my_callback2, captured_matches, ruleset_data
        )


@pytest.mark.asyncio
async def test_ha_timed_out_grace_period(db_params):
    """Test TimedOut with grace period surviving a failover.

    Partial match (code=1001) at T=0, timeout=10s, crash at
    T=8s, Node2 clock at T=15s (5s past timeout). Grace=600s.
    The match should be captured and dispatched as a recovery
    event.
    """

    ha_uuid = str(uuid.uuid4())
    ha_config = {"expired_window_grace_period": 600}
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_not_all_grace_period_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "maint failed"

    # --- Worker 1: send partial match, advance to T=8s ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        ha_config,
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send partial match: code=1001 (only 1 of 2)
        rs1.assert_event(
            json.dumps(
                {
                    "alert": {
                        "code": 1001,
                        "message": "Applying maintenance",
                    }
                }
            )
        )
        print("worker-1 sent partial match event")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Advance to T=8s (timeout at T=10s)
        rs1.advance_time(8, "Seconds")
        print("worker-1 advanced time by 8 seconds")

        # Rule should NOT have fired (timeout not expired and
        # code=1002 could still arrive)
        assert_rule_not_fired(
            my_callback1, [], "Rule should not fire before timeout expires"
        )
        print("Confirmed: rule did not fire on worker-1")

    print("RulesetCollection shut down")

    # --- Worker 2: advance to T=15s, then take over ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        ha_config,
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # The timeout expires during clock jump
        # (5s past timeout, within 600s grace)
        await verify_grace_period_recovery(
            rs2, my_callback2, captured_matches, ruleset_data
        )


@pytest.mark.asyncio
async def test_ha_encryption_failover(db_params):
    """Test HA with encryption enabled across a failover.

    Worker-1 initializes HA with an encryption key, fires a rule,
    and persists encrypted state. Worker-1 goes down. Worker-2
    takes over with the same encryption key, recovers the encrypted
    matching event via async channel, and verifies action info
    created by worker-1 is readable.
    """

    ha_uuid = str(uuid.uuid4())
    encryption_key = generate_encryption_key()
    print(f"HA Cluster UUID: {ha_uuid}")

    enc_ha_config = {
        "encryption_key_primary": encryption_key,
    }

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    # --- Worker 1: fire rule with encryption, then go down ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        enc_ha_config,
        ruleset_data,
        "assignment",
        captured_matches,
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]
        async_task1 = ctx["async_task"]

        print("worker-1 became a Leader (encryption enabled)")

        # Assert an event that will match the rule
        rs1.assert_event(json.dumps({"i": 67}))

        # Wait for async task
        try:
            await cancel_async_task(async_task1)

        except asyncio.CancelledError:  # NOSONAR
            pass  # Expected when cancelling

        # Verify the callback was called
        assert_rule_fired(my_callback1, captured_matches)

        matching_uuid = captured_matches[0].matching_uuid
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"
        print(f"Rule fired on worker-1! matching_uuid: {matching_uuid}")

        # Add action info (persisted with encryption)
        action_data = json.dumps({"action": "print_event", "status": "1"})
        add_action_info(ruleset_data["name"], matching_uuid, 0, action_data)
        assert action_info_exists(ruleset_data["name"], matching_uuid, 0)
        print("worker-1 added action info (encrypted)")

        # Verify raw DB columns are encrypted
        raw_event_data = query_raw_column(
            db_params,
            "SELECT event_data FROM drools_ansible_matching_event "
            "WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_event_data is not None, "matching_event row should exist"
        assert raw_event_data.startswith(
            "$ENCRYPTED$"
        ), f"event_data should be encrypted, got: {raw_event_data[:50]}"
        print(f"Verified: event_data is encrypted {raw_event_data[:30]}...")

        raw_session_state = query_raw_column(
            db_params,
            "SELECT partial_matching_events FROM "
            "drools_ansible_session_state WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_session_state is not None, "session_state row should exist"
        assert raw_session_state.startswith("$ENCRYPTED$"), (
            f"partial_matching_events should be encrypted, "
            f"got: {raw_session_state[:50]}"
        )
        print(
            "Verified: session_state is encrypted "
            f"({raw_session_state[:30]}...)"
        )

    print("RulesetCollection shut down")

    # --- Worker 2: take over with same encryption key ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        enc_ha_config,
        ruleset_data,
        "assignment",
        [],
        auto_enable_leader=False,
    ) as ctx:
        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader (encryption enabled)")

        # Wait for recovery of encrypted state
        print("Waiting for recovery...")
        await wait_for_async_processing(0.5)

        # Worker-2 should be able to read action info created by
        # worker-1 (encrypted in DB)
        assert action_info_exists(ruleset_data["name"], matching_uuid, 0)
        retrieved = get_action_info(ruleset_data["name"], matching_uuid, 0)
        assert retrieved == action_data
        print("worker-2 successfully read encrypted action info from worker-1")

        # Clean up
        delete_action_info(ruleset_data["name"], matching_uuid)
        print("worker-2 cleaned up")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()


@pytest.mark.asyncio
async def test_ha_key_rotation_with_restart(db_params):
    """Test encryption key rotation across a graceful restart.

    Worker-1 starts as leader with the original encryption key,
    fires a rule, and persists encrypted state. Worker-1 shuts
    down (graceful restart, not failover). Worker-1 restarts with
    a rotated key pair (new primary + old key as secondary).
    Recovery decrypts using the secondary key fallback. A
    non-matching event triggers session state re-encryption with
    the new primary key. Finally we verify that the re-encrypted
    session state can be decrypted with only the new key.
    """

    ha_uuid = str(uuid.uuid4())
    original_key = generate_encryption_key()
    new_key = generate_encryption_key()
    print(f"HA Cluster UUID: {ha_uuid}")

    original_ha_config = {
        "encryption_key_primary": original_key,
    }

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    captured_matches = []

    # === Phase 1: Worker-1 as leader with original key ===
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        original_ha_config,
        ruleset_data,
        "assignment",
        captured_matches,
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]
        async_task1 = ctx["async_task"]

        print("worker-1 became a Leader (original key)")

        # Assert an event that will match the rule
        rs1.assert_event(json.dumps({"i": 67}))

        # Wait for async task
        try:
            await cancel_async_task(async_task1)

        except asyncio.CancelledError:  # NOSONAR
            pass  # Expected when cancelling

        # Verify the callback was called
        assert_rule_fired(my_callback1, captured_matches)

        matching_uuid = captured_matches[0].matching_uuid
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"
        print(f"Rule fired on worker-1! matching_uuid: {matching_uuid}")

        # Verify raw DB column is encrypted with original key
        raw_event_data = query_raw_column(
            db_params,
            "SELECT event_data FROM drools_ansible_matching_event "
            "WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_event_data is not None, "matching_event row should exist"
        assert raw_event_data.startswith(
            "$ENCRYPTED$"
        ), f"event_data should be encrypted, got: {raw_event_data[:50]}"
        print(f"Verified: event_data is encrypted ({raw_event_data[:30]}...)")

    print("=== Restarting with rotated keys ===")

    # === Phase 2: Restart with rotated keys ===
    rotated_ha_config = {
        "encryption_key_primary": new_key,
        "encryption_key_secondary": original_key,
    }

    recovered_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-1-rotated",
        db_params,
        rotated_ha_config,
        ruleset_data,
        "assignment",
        recovered_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]

        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-1-rotated became a Leader (new primary + old secondary)")

        # Wait for recovery — should decrypt with secondary key
        print("Waiting for recovery...")
        await wait_for_async_processing(1.0)

        # Assert a non-matching event to trigger session state persist,
        # which re-encrypts with the new primary key
        rs2.assert_event(json.dumps({"i": 1}))
        print("Asserted non-matching event to trigger re-encryption")

        # Give time for persist
        await wait_for_async_processing(0.5)

        # Verify session state is re-encrypted with new key
        raw_session_state = query_raw_column(
            db_params,
            "SELECT partial_matching_events FROM "
            "drools_ansible_session_state WHERE ha_uuid = ? "
            "AND rule_set_name = ?",
            ha_uuid,
            ruleset_data["name"],
        )
        assert (
            raw_session_state is not None
        ), "session_state row should exist after re-encryption"
        assert raw_session_state.startswith("$ENCRYPTED$"), (
            f"session_state should be encrypted, got: "
            f"{raw_session_state[:50]}"
        )
        print(
            "Verified: session_state is encrypted "
            f"({raw_session_state[:30]}...)"
        )

        # Decrypt with only the new key (no secondary) to confirm
        # re-encryption used the new primary key
        import jpy

        ha_encryption = jpy.get_type(
            "org.drools.ansible.rulebook.integration.ha.api.HAEncryption"
        )
        rotated_only = ha_encryption(new_key, None)
        decrypt_result = rotated_only.decrypt(raw_session_state)
        assert (
            not decrypt_result.usedSecondaryKey()
        ), "Should decrypt with primary (new) key, not secondary"
        assert (
            decrypt_result.plaintext() is not None
        ), "Decrypted session state should not be None"
        print("Verified: session state re-encrypted with new primary key")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()


@pytest.mark.asyncio
async def test_ha_get_partial_event_ids_survives_failover(
    db_params,
):
    """Test getPartialEventIds API survives failover.

    Worker-1 sends two partial events (only first condition
    of a two-condition AllCondition rule), then goes down.
    Worker-2 takes over and verifies that getPartialEventIds
    returns the same event UUIDs after recovery.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast(
        "asts/rules_with_multiple_conditions_all_assignment.yml"
    )
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "multiple conditions"

    event_uuid1 = str(uuid.uuid4())
    event_uuid2 = str(uuid.uuid4())

    # --- Worker 1: send partial events ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send two events satisfying only the first condition (i==0)
        # with explicit UUIDs
        event1 = json.dumps({"meta": {"uuid": event_uuid1}, "i": 0})
        event2 = json.dumps({"meta": {"uuid": event_uuid2}, "i": 0})
        rs1.assert_event(event1)
        rs1.assert_event(event2)
        print("worker-1 sent two partial events")

        await wait_for_async_processing(0.5)

        # Rule should not have fired
        assert_rule_not_fired(
            my_callback1,
            [],
            "Rule should not fire with only one condition matched",
        )

        # Verify partial event IDs on worker-1
        partial_ids = get_partial_event_ids(ruleset_data["name"])
        print(f"Partial event IDs on worker-1: {partial_ids}")
        assert len(partial_ids) == 2
        assert set(partial_ids) == {event_uuid1, event_uuid2}

    print("RulesetCollection shut down")

    # --- Worker 2: takes over and verifies partial IDs ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        auto_enable_leader=False,
    ) as ctx:
        await wait_for_async_processing(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        await wait_for_async_processing(0.5)

        # Verify partial event IDs recovered on worker-2
        partial_ids = get_partial_event_ids(ruleset_data["name"])
        print(f"Partial event IDs on worker-2 (after failover): {partial_ids}")
        assert len(partial_ids) == 2
        assert set(partial_ids) == {event_uuid1, event_uuid2}
        print("Partial event IDs survived failover!")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()


@pytest.mark.asyncio
async def test_ha_all_timeout_expired_during_outage(db_params):
    """Test AllCondition+timeout where time window expires during outage.

    Worker-1 sends event i (partial match) at T=0, advances to T=8s,
    then goes down. Worker-2 takes over with its clock at T=15s
    (5s past the 10s timeout). With default expired_window_grace_period=0,
    the expired time window is not recoverable. A WARN message is logged
    and the rule does NOT fire on worker-2.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast("asts/test_all_timeout_ha_ast.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "r1"

    # --- Worker 1: send event i, advance to T=8s, go down ---
    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        db_params,
        {},
        ruleset_data,
        rule_name,
        [],
        shutdown_on_exit=True,
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("worker-1 became a Leader")

        # Send event i at T=0 — starts the AllCondition timer
        rs1.assert_event(json.dumps({"i": 42, "host": "A"}))
        print("worker-1 sent event {i: 42}")

        # Give async processing time
        await wait_for_async_processing(0.5)

        # Advance to T=8s (still within 10s window)
        rs1.advance_time(8, "Seconds")
        print("worker-1 advanced time by 8 seconds")

        # Rule should NOT have fired (j hasn't arrived yet)
        assert_rule_not_fired(
            my_callback1, [], "Rule should not fire with partial match"
        )
        print("Confirmed: rule did not fire on worker-1")

    print("RulesetCollection shut down")

    # --- Worker 2: clock at T=15s (past 10s timeout), take over ---
    captured_matches = []
    async with ha_worker_manager(
        ha_uuid,
        "worker-2",
        db_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
        auto_enable_leader=False,
    ) as ctx:
        rs2 = ctx["ruleset"]
        my_callback2 = ctx["callback"]

        # Advance Node2 clock to T=15s BEFORE becoming leader
        rs2.advance_time(15, "Seconds")

        # Yield to ensure async handler starts reading
        await wait_for_async_processing(0.1)

        # Node2 becomes leader — recovery advances clock from T=8s to T=15s
        # The AllCondition timeout expires during this jump
        # With grace_period=0, the expired window is dropped and
        # a WARN message is logged
        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery
        print("Waiting for recovery...")
        await wait_for_async_processing(1.0)

        # The rule should NOT fire: time window expired and
        # grace_period=0 means no recovery
        assert_rule_not_fired(
            my_callback2,
            captured_matches,
            "Rule should not fire — time window expired with no grace period",
        )
        print(
            "Confirmed: rule did not fire on worker-2 "
            "(time window expired, WARN logged)"
        )

        # Even sending event j now should not trigger the rule
        # because the time window already expired
        rs2.assert_event(json.dumps({"j": 99, "host": "B"}))
        print("worker-2 sent event {j: 99}")
        await wait_for_async_processing(0.5)

        assert (
            len(captured_matches) == 0
        ), "Rule should not fire — original time window already expired"
        print("Confirmed: late event j did not trigger rule")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()


@pytest.mark.asyncio
async def test_ha_overwrite_if_rulebook_changes(db_params):
    """Test overwrite_if_rulebook_changes=false on same node.

    V1 ruleset: AllCondition requiring events i AND j.
    V2 ruleset: AllCondition requiring events i AND k
    (same ruleset name, different conditions).

    Single node starts with V1, sends event i (partial match),
    disposes the session, then creates a new session with V2.
    With overwrite_if_rulebook_changes=false, the partial event i
    from V1 is recovered despite the rulebook hash mismatch.
    Sending event k completes the V2 rule.
    """

    ha_uuid = str(uuid.uuid4())
    ha_config = {"overwrite_if_rulebook_changes": False}
    print(f"HA Cluster UUID: {ha_uuid}")

    v1_data = load_ast("asts/test_ruleset_update_v1_ast.yml")
    v1_ruleset = v1_data[0]["RuleSet"]

    v2_data = load_ast("asts/test_ruleset_update_v2_ast.yml")
    v2_ruleset = v2_data[0]["RuleSet"]

    rule_name = "r1"
    captured_matches = []

    # --- Phase 1: V1 ruleset, send event i (partial) ---
    async with ha_worker_manager(
        ha_uuid, "worker-1", db_params, ha_config, v1_ruleset, rule_name, []
    ) as ctx:
        rs1 = ctx["ruleset"]
        my_callback1 = ctx["callback"]

        print("Phase 1: Leader with V1 ruleset")

        # Send event i — partial match (V1 needs i AND j)
        rs1.assert_event(json.dumps({"i": 42, "host": "A"}))
        print("Sent event {i: 42}")

        await wait_for_async_processing(0.5)

        assert_rule_not_fired(
            my_callback1, [], "Rule should not fire with partial match"
        )
        print("Confirmed: rule did not fire (partial)")

        # Dispose V1 session (keeps persisted state in DB)
        rs1.end_session()
        print("V1 session disposed")

        # --- Phase 2: Create V2 ruleset on same node ---
        rs2, my_callback2 = create_ruleset_with_callback(
            v2_ruleset, rule_name, captured_matches, None
        )

        print(
            "Phase 2: Created V2 ruleset (overwrite_if_rulebook_changes=false)"
        )

        # Wait for recovery of partial event i from V1
        await wait_for_async_processing(0.5)

        # Rule should NOT have fired yet (only i recovered, V2 needs i AND k)
        assert_rule_not_fired(
            my_callback2,
            captured_matches,
            "Rule should not fire with only recovered partial event i",
        )
        print("Confirmed: rule did not fire yet")

        # Send event k — completes V2 AllCondition
        rs2.assert_event(json.dumps({"k": 99, "host": "B"}))
        print("Sent event {k: 99}")

        await wait_for_async_processing(0.5)

        # Rule should fire: recovered i from V1 + new k
        assert_rule_fired(
            my_callback2,
            captured_matches,
            "Rule should fire — recovered i from V1 and new k "
            "complete V2 AllCondition",
        )
        print(
            f"Rule fired! matching_uuid: {captured_matches[0].matching_uuid}"
        )

        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Verify match contains event i from V1 recovery
        assert "m_0" in match_data
        assert match_data["m_0"]["i"] == 42

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(v2_ruleset["name"], matching_uuid)
        rs2.end_session()
        print("Cleaned up")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()


@pytest.mark.asyncio
async def test_ha_postgres_multi_host(db_params):
    """Test PostgreSQL multi-host connection support.

    Uses two host:port pairs where the first one is
    unreachable (dead port). The JDBC driver should fail
    on the first host and seamlessly connect to the second.
    Just running successfully confirms multi-host failover.

    Skipped when using H2.
    """

    if db_params["db_type"] != "postgres":
        pytest.skip("Multi-host test requires PostgreSQL")

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    real_host = db_params["host"]
    real_port = str(db_params["port"])

    # Build multi-host params: first host is unreachable, second is
    # the real PostgreSQL instance
    multi_host_params = dict(db_params)
    multi_host_params["host"] = f"{real_host},{real_host}"
    multi_host_params["port"] = f"59999,{real_port}"

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "assignment"
    captured_matches = []

    async with ha_worker_manager(
        ha_uuid,
        "worker-1",
        multi_host_params,
        {},
        ruleset_data,
        rule_name,
        captured_matches,
    ) as ctx:
        my_callback = ctx["callback"]

        print(f"Leader enabled with multi-host (dead:59999, live:{real_port})")

        # Send an event that matches the rule
        ctx["ruleset"].assert_event(json.dumps({"i": 67}))
        print("Sent event {i: 67}")

        await wait_for_async_processing(0.5)

        # Rule should fire — confirms DB connection worked
        assert_rule_fired(
            my_callback,
            captured_matches,
            "Rule should fire — multi-host connection should "
            "have failed over to the second host",
        )
        print(
            f"Rule fired! matching_uuid: {captured_matches[0].matching_uuid}"
        )

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        print("Multi-host test passed")

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()
