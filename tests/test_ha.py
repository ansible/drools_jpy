import asyncio
import base64
import json
import os
import uuid
from unittest import mock

import pytest
import yaml

from drools.dispatch import (
    establish_async_channel,
    handle_async_messages,
)
from drools.rule import Rule
from drools.ruleset import (
    Ruleset,
    RulesetCollection,
    action_info_exists,
    add_action_info,
    delete_action_info,
    disable_leader,
    enable_leader,
    get_action_info,
    get_action_status,
    get_ha_stats,
    get_partial_event_ids,
    initialize_ha,
    shutdown,
    update_action_info,
)


def load_ast(filename: str) -> dict:
    test_dir = os.path.dirname(os.path.realpath(__file__))
    with open(f"{test_dir}/{filename}") as f:
        test_data = yaml.safe_load(f)
    return test_data


@pytest.fixture
def db_params():
    """DB connection parameters for testing"""
    return {
        "db_type": os.environ.get(
            "DROOLS_HA_DB_TYPE", "h2"
        ),  # "h2" or "postgres"
        "db_file_path": os.environ.get(
            "DROOLS_HA_H2_FILE", "./eda_ha"
        ),  # Only used for H2, ignored for PostgreSQL
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "eda_ha_db"),
        "user": os.environ.get("POSTGRES_USER", "eda_user"),
        "password": os.environ.get("POSTGRES_PASSWORD", "eda_password"),
    }


@pytest.fixture
def ha_config():
    """HA configuration parameters"""
    return {
        "write_after": 1,  # not yet implemented
        "dedup_buffer_size": 5, # default is 5
    }


@pytest.mark.asyncio
async def test_ha_initialization_and_leader_lifecycle(db_params, ha_config):
    """Test HA initialization, createRuleset, enableLeader, and assertEvent"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        # 1. Initialize HA with a unique UUID
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, "worker-1", db_params, ha_config)

        # 2. Create a ruleset
        test_data = load_ast("asts/rules_with_assignment.yml")
        ruleset_data = test_data[0]["RuleSet"]

        # Capture the actual match data
        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            # Cancel async task after first match
            async_task.cancel()

        my_callback = mock.Mock(side_effect=capture_callback)
        rs = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs.add_rule(Rule("assignment", my_callback))

        # 3. Enable leader mode
        enable_leader()

        # Verify HA stats after enabling leader
        ha_stats = get_ha_stats()
        print(f"HA Stats after enabling leader: {ha_stats}")
        assert ha_stats is not None
        assert isinstance(ha_stats, dict)

        # 4. Assert an event that will match the rule
        rs.assert_event(json.dumps(dict(i=67)))

        ha_stats = get_ha_stats()
        print(f"HA Stats after sending an event: {ha_stats}")

        # Wait for async task to complete
        try:
            await async_task
        except asyncio.CancelledError:
            pass

        # Verify the callback was called
        assert my_callback.called
        assert len(captured_matches) > 0

        # 5. Extract matching_uuid from the Matches object
        # The matching_uuid is now directly available in the Matches object
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

        # 6. Disable leader mode
        disable_leader()

        # 7. Clean up
        rs.end_session()
    finally:
        # Ensure async task is cancelled
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_action_info_lifecycle(db_params):
    """Test ActionInfo CRUD operations"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        # Initialize HA
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, "worker-1", db_params, {})

        # Create a ruleset
        test_data = load_ast("asts/rules_with_assignment.yml")
        ruleset_data = test_data[0]["RuleSet"]

        # Capture the actual match data to get matching_uuid
        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task.cancel()

        my_callback = mock.Mock(side_effect=capture_callback)
        rs = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs.add_rule(Rule("assignment", my_callback))

        # Enable leader
        enable_leader()

        # Assert an event to generate a match with matching_uuid
        rs.assert_event(json.dumps(dict(i=67)))

        # Wait for async task
        try:
            await async_task
        except asyncio.CancelledError:
            pass

        # Verify callback was called
        assert my_callback.called
        assert len(captured_matches) > 0

        # Extract matching_uuid from the Matches object
        matching_uuid = captured_matches[0].matching_uuid

        # Ensure we have a matching_uuid (should be present in HA mode)
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"

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

        # Clean up
        disable_leader()
        rs.end_session()
    finally:
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_ha_stats(db_params):
    """Test getting HA statistics"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, "worker-1", db_params, {})

        # Create a ruleset
        test_data = load_ast("asts/rules_with_assignment.yml")
        ruleset_data = test_data[0]["RuleSet"]

        # Capture the actual match data to get matching_uuid
        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task.cancel()

        my_callback = mock.Mock(side_effect=capture_callback)
        rs = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs.add_rule(Rule("assignment", my_callback))

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

        # Clean up
        disable_leader()
    finally:
        async_task.cancel()
        try:
            await async_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_multiple_action_infos(db_params):
    """Test managing multiple actions for the same matching event"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, "worker-1", db_params, {})

        test_data = load_ast("asts/rules_with_assignment.yml")
        ruleset_data = test_data[0]["RuleSet"]

        # Capture the actual match data to get matching_uuid
        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task.cancel()

        my_callback = mock.Mock(side_effect=capture_callback)
        rs = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs.add_rule(Rule("assignment", my_callback))

        enable_leader()

        # Assert an event to generate a match with matching_uuid
        rs.assert_event(json.dumps(dict(i=67)))

        # Wait for async task
        try:
            await async_task
        except asyncio.CancelledError:
            pass

        # Verify callback was called
        assert my_callback.called
        assert len(captured_matches) > 0

        # Extract matching_uuid from the Matches object
        matching_uuid = captured_matches[0].matching_uuid

        # Ensure we have a matching_uuid (should be present in HA mode)
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"

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

        disable_leader()
        rs.end_session()
    finally:
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_ha_failover_scenario(db_params):
    """Test HA failover scenario with leader switch"""

    # Use the same HA UUID for both instances
    # (they're part of the same HA cluster)
    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    # Leader 1 starts
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        test_data = load_ast("asts/rules_with_assignment.yml")
        ruleset_data = test_data[0]["RuleSet"]

        # Capture matches to get real matching_uuid
        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task1.cancel()

        my_callback1 = mock.Mock(side_effect=capture_callback)
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule("assignment", my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Leader 1 asserts an event to get a real matching_uuid
        rs1.assert_event(json.dumps(dict(i=67)))

        # Wait for match
        try:
            await async_task1
        except asyncio.CancelledError:
            pass

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

        # Leader 1 goes down
        disable_leader()
        rs1.end_session()
        print("Leader worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown the RulesetCollection to close the
    # AstRulesEngine and async server socket.
    # This allows Leader 2 to create a new engine.
    print(
        "Shutting down RulesetCollection" " (simulating Leader 1 JVM shutdown)"
    )
    shutdown()
    print("RulesetCollection shut down")

    # Leader 2 starts with the SAME HA UUID
    # (part of the same cluster).
    # This will create a new AstRulesEngine instance.
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        # Use the same ha_uuid to represent the same HA cluster
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule("assignment", mock.Mock()))

        # Yield to ensure async handler starts reading
        # before enableLeader sends the message
        print("Yielding to async handler before enabling leader...")
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for potential recovery message
        print("Waiting for recovery message...")
        await asyncio.sleep(0.5)

        # Leader 2 should be able to read the action info created by Leader 1
        assert action_info_exists(ruleset_data["name"], matching_uuid, 0)
        print("Leader worker-2 successfully read action info from worker-1")

        stats_after_failover = get_ha_stats()
        print(f"HA Stats after failover: {stats_after_failover}")

        # Clean up
        delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("Leader worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_ha_failover_with_partial_match(db_params):
    """Test HA failover: partial match completed by
    worker-2.

    Uses rules_with_multiple_conditions_all_assignment
    which has an AllCondition requiring two events:
    first (i==0) and second (i==1). Worker-1 sends only
    the first event, then goes down. Worker-2 takes over
    and sends the second event, which should fire the
    rule thanks to HA state recovery.
    """

    ha_uuid = str(uuid.uuid4())
    print(f"HA Cluster UUID: {ha_uuid}")

    test_data = load_ast(
        "asts/rules_with_multiple_conditions_all_assignment.yml"
    )
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "multiple conditions"

    # --- Worker 1: send partial match (first condition only) ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Send first event (i==0) — matches only the
        # first condition, rule should NOT fire
        rs1.assert_event(json.dumps(dict(i=0)))
        print("worker-1 sent event {i: 0} (partial match)")

        # Give some time for async processing
        await asyncio.sleep(0.5)

        # Rule should not have fired (only one of two conditions met)
        assert (
            not my_callback1.called
        ), "Rule should not fire with only one condition matched"
        print("Confirmed: rule did not fire on worker-1 (partial match)")

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM going down.
    # Note: handle_async_messages may have already called
    # shutdown() via its CancelledError handler when
    # async_task1 was cancelled in the finally block.
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection (simulating worker-1 shutdown)")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: takes over and completes the match ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback2 = mock.Mock(side_effect=capture_callback)
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Yield to ensure async handler starts reading before enableLeader
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of the partial match from worker-1
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

        # Send second event (i==1) — completes the
        # AllCondition, rule should fire.
        # Fires synchronously via _process_response,
        # no need to await async task.
        rs2.assert_event(json.dumps(dict(i=1)))
        print("worker-2 sent event {i: 1} (completing the match)")

        # Verify the rule fired on worker-2
        assert (
            my_callback2.called
        ), "Rule should fire after both conditions are met"
        assert len(captured_matches) > 0
        matching_uuid = captured_matches[0].matching_uuid
        print(f"Rule fired on worker-2! matching_uuid: {matching_uuid}")

        # Verify match data contains both events
        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Clean up while engine is still alive
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


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

    # --- Worker 1: fire rule, add action infos, update one, then go down ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback1 = mock.Mock(side_effect=capture_callback)
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # 1. Send event to fire the rule (i==1)
        rs1.assert_event(json.dumps(dict(i=1)))
        print("worker-1 sent event {i: 1}")

        # Verify the rule fired
        assert my_callback1.called, "Rule should fire when i==1"
        assert len(captured_matches) > 0

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

        # 5. Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM going down.
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection (simulating worker-1 shutdown)")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: takes over, receives recovery,
    # updates remaining actions ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        my_callback2 = mock.Mock()
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Yield to ensure async handler starts reading before enableLeader
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # 6. Wait for MatchingEvent recovery
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

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

        # 10. Cleanup worker-2
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


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
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Send 5 events — once_after window is 2 seconds,
        # so the rule won't fire until time is advanced
        for i in range(5):
            rs1.assert_event(json.dumps(dict(i=i, meta=dict(host="A"))))
        print("worker-1 sent 5 events")

        # Give async processing time
        await asyncio.sleep(0.5)

        # Rule should NOT have fired (still within once_after window)
        assert (
            not my_callback1.called
        ), "Rule should not fire before once_after window expires"
        print("Confirmed: rule did not fire on worker-1")

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM going down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: take over, recover state, advance time ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback2 = mock.Mock(side_effect=capture_callback)
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of buffered state from worker-1
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

        # Advance time past the once_after window (2 seconds)
        rs2.advance_time(3, "Seconds")
        print("worker-2 advanced time by 3 seconds")

        # Wait for async match delivery
        await asyncio.sleep(1.0)

        # The rule should have fired after time advanced past window
        assert (
            my_callback2.called
        ), "Rule should fire after once_after window expires"
        assert len(captured_matches) > 0
        print(
            f"Rule fired on worker-2! "
            f"matching_uuid: {captured_matches[0].matching_uuid}"
        )

        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_ha_duplicate_event_detection_survives_failover(db_params, ha_config):
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

    # --- Worker 1: process event, then go down ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, ha_config)

        captured_matches_1 = []

        def capture_callback_1(matches):
            captured_matches_1.append(matches)

        my_callback1 = mock.Mock(side_effect=capture_callback_1)
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
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
        await asyncio.sleep(0.5)

        # Rule should have fired (temperature 35 > 30)
        assert my_callback1.called, "Rule should fire for temperature > 30"
        assert len(captured_matches_1) > 0
        print(
            f"Rule fired on worker-1! "
            f"matching_uuid: "
            f"{captured_matches_1[0].matching_uuid}"
        )

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM going down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: take over, re-send same event (should be ignored) ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        captured_matches_2 = []

        def capture_callback_2(matches):
            captured_matches_2.append(matches)

        my_callback2 = mock.Mock(side_effect=capture_callback_2)
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery (may dispatch recovery events)
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

        # Reset tracking after recovery so recovery events
        # don't interfere with duplicate detection checks
        my_callback2.reset_mock()
        captured_matches_2.clear()
        print("Reset mock after recovery")

        # Re-send the same event (same UUID) — should be
        # detected as duplicate and ignored
        event = json.dumps(
            {
                "meta": {"uuid": dup_event_uuid},
                "temperature": 35,
            }
        )
        rs2.assert_event(event)
        print("worker-2 re-sent duplicate event " "(same UUID as worker-1)")

        # Give async processing time
        await asyncio.sleep(0.5)

        # Rule should NOT fire — event is a duplicate
        assert (
            not my_callback2.called
        ), "Duplicate event should be ignored after failover"
        assert len(captured_matches_2) == 0
        print("Confirmed: duplicate event was ignored " "on worker-2")

        # Send a NEW event with a different UUID —
        # should be processed normally
        new_event = json.dumps(
            {
                "meta": {"uuid": new_event_uuid},
                "temperature": 40,
            }
        )
        rs2.assert_event(new_event)
        print("worker-2 sent new event with temperature=40")

        # Give async processing time
        await asyncio.sleep(0.5)

        # Rule should fire for the new event
        assert my_callback2.called, "New event should be processed normally"
        assert len(captured_matches_2) > 0
        print(
            f"Rule fired on worker-2 for new event! "
            f"matching_uuid: "
            f"{captured_matches_2[0].matching_uuid}"
        )

        # Clean up
        matching_uuid = captured_matches_2[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


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
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(handle_async_messages(reader1, writer1))

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Send event i — starts the NotAllCondition timer
        rs1.assert_event(json.dumps(dict(i=42, host="A")))
        print("worker-1 sent event {i: 42}")

        # Give async processing time
        await asyncio.sleep(0.5)

        # Rule should NOT have fired yet (timeout hasn't expired
        # and j could still arrive)
        assert (
            not my_callback1.called
        ), "Rule should not fire before timeout expires"
        print("Confirmed: rule did not fire on worker-1")

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: take over, recover state, advance time ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(handle_async_messages(reader2, writer2))

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback2 = mock.Mock(side_effect=capture_callback)
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery of partial match from worker-1
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

        # Advance time past the NotAllCondition timeout
        rs2.advance_time(3, "Seconds")
        print("worker-2 advanced time by 3 seconds")

        # Wait for async match delivery
        await asyncio.sleep(1.0)

        # The rule should fire: event i arrived but j never did
        assert (
            my_callback2.called
        ), "Rule should fire after timeout (j never arrived)"
        assert len(captured_matches) > 0
        print(
            f"Rule fired on worker-2! "
            f"matching_uuid: "
            f"{captured_matches[0].matching_uuid}"
        )

        match_data = captured_matches[0].data
        print(f"Match data: {match_data}")

        # Verify match contains event i from worker-1
        assert "m_0" in match_data
        assert match_data["m_0"]["i"] == 42

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


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

    test_data = load_ast(
        "asts/test_once_after_grace_period_ast.yml"
    )
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "alert_throttle"

    # --- Worker 1: send event, advance to T=8s, go down ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(
        handle_async_messages(reader1, writer1)
    )

    try:
        initialize_ha(
            ha_uuid, "worker-1", db_params, ha_config
        )

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Send event at T=0
        rs1.assert_event(
            json.dumps(
                {"alert": {"type": "warning", "host": "h1"}}
            )
        )
        print("worker-1 sent alert event")

        # Give async processing time
        await asyncio.sleep(0.5)

        # Advance to T=8s (still within 10s window)
        rs1.advance_time(8, "Seconds")
        print("worker-1 advanced time by 8 seconds")

        # Rule should NOT have fired yet
        assert not my_callback1.called, (
            "Rule should not fire before "
            "once_after window expires"
        )
        print("Confirmed: rule did not fire on worker-1")

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: advance to T=15s, then take over ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(
        handle_async_messages(reader2, writer2)
    )

    try:
        initialize_ha(
            ha_uuid, "worker-2", db_params, ha_config
        )

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback2 = mock.Mock(
            side_effect=capture_callback
        )
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Advance Node2 clock to T=15s BEFORE becoming
        # leader so recovery will jump from T=8s to T=15s
        rs2.advance_time(15, "Seconds")

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        # Node2 becomes leader — recovery advances clock
        # from T=8s (persisted) to T=15s (Node2 clock).
        # The once_after timer expires during this clock
        # jump (5s past expiry, within 600s grace).
        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery and async delivery
        print("Waiting for recovery...")
        await asyncio.sleep(1.0)

        # The match should have been dispatched
        assert my_callback2.called, (
            "Grace period match should be dispatched "
            "via async channel"
        )
        assert len(captured_matches) > 0
        print(
            f"Rule fired on worker-2! "
            f"matching_uuid: "
            f"{captured_matches[0].matching_uuid}"
        )

        # After recovery, advancing time should NOT
        # produce a duplicate match
        initial_count = len(captured_matches)
        rs2.advance_time(5, "Seconds")
        await asyncio.sleep(0.5)
        assert len(captured_matches) == initial_count, (
            "No duplicate match should fire "
            "after grace period recovery"
        )
        print("Confirmed: no duplicate match")

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(
                ruleset_data["name"], matching_uuid
            )
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


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

    test_data = load_ast(
        "asts/test_not_all_grace_period_ast.yml"
    )
    ruleset_data = test_data[0]["RuleSet"]
    rule_name = "maint failed"

    # --- Worker 1: send partial match, advance to T=8s ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(
        handle_async_messages(reader1, writer1)
    )

    try:
        initialize_ha(
            ha_uuid, "worker-1", db_params, ha_config
        )

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
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
        await asyncio.sleep(0.5)

        # Advance to T=8s (timeout at T=10s)
        rs1.advance_time(8, "Seconds")
        print("worker-1 advanced time by 8 seconds")

        # Rule should NOT have fired (timeout not expired
        # and code=1002 could still arrive)
        assert not my_callback1.called, (
            "Rule should not fire before timeout expires"
        )
        print("Confirmed: rule did not fire on worker-1")

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: advance to T=15s, then take over ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(
        handle_async_messages(reader2, writer2)
    )

    try:
        initialize_ha(
            ha_uuid, "worker-2", db_params, ha_config
        )

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)

        my_callback2 = mock.Mock(
            side_effect=capture_callback
        )
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        # Advance Node2 clock to T=15s BEFORE becoming
        # leader so recovery will jump from T=8s to T=15s
        rs2.advance_time(15, "Seconds")

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        # Node2 becomes leader — recovery advances clock
        # from T=8s (persisted) to T=15s (Node2 clock).
        # The timeout expires during this clock jump
        # (5s past timeout, within 600s grace).
        enable_leader()
        print("worker-2 became a Leader")

        # Wait for recovery and async delivery
        print("Waiting for recovery...")
        await asyncio.sleep(1.0)

        # The match should have been dispatched
        assert my_callback2.called, (
            "Grace period match should be dispatched "
            "via async channel"
        )
        assert len(captured_matches) > 0
        print(
            f"Rule fired on worker-2! "
            f"matching_uuid: "
            f"{captured_matches[0].matching_uuid}"
        )

        # After recovery, advancing time should NOT
        # produce a duplicate match
        initial_count = len(captured_matches)
        rs2.advance_time(5, "Seconds")
        await asyncio.sleep(0.5)
        assert len(captured_matches) == initial_count, (
            "No duplicate match should fire "
            "after grace period recovery"
        )
        print("Confirmed: no duplicate match")

        # Clean up
        matching_uuid = captured_matches[0].matching_uuid
        if matching_uuid:
            delete_action_info(
                ruleset_data["name"], matching_uuid
            )
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass


def _generate_encryption_key():
    """Generate a random 256-bit AES key, base64-encoded."""
    key_bytes = os.urandom(32)
    return base64.b64encode(key_bytes).decode("ascii")


def _query_raw_column(db_params, sql, *param_values):
    """Query a raw column value from DB via JDBC through JPY.

    Bypasses the state manager's decryption so we can verify
    data is actually encrypted at rest.
    """
    import jpy

    DriverManager = jpy.get_type("java.sql.DriverManager")

    db_type = db_params.get("db_type", "h2")
    if db_type == "h2":
        db_file = db_params.get("db_file_path", "./eda_ha")
        jdbc_url = (
            f"jdbc:h2:file:{db_file};MODE=PostgreSQL"
        )
        conn = DriverManager.getConnection(jdbc_url, "SA", "")
    else:
        host = db_params.get("host", "localhost")
        port = db_params.get("port", 5432)
        database = db_params.get("database", "eda_ha_db")
        user = db_params.get("user", "eda_user")
        password = db_params.get("password", "eda_password")
        jdbc_url = (
            f"jdbc:postgresql://{host}:{port}/{database}"
        )
        conn = DriverManager.getConnection(
            jdbc_url, user, password
        )

    try:
        stmt = conn.prepareStatement(sql)
        for i, val in enumerate(param_values, 1):
            stmt.setString(i, val)
        rs = stmt.executeQuery()
        if rs.next():
            return rs.getString(1)
        return None
    finally:
        conn.close()


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
    encryption_key = _generate_encryption_key()
    print(f"HA Cluster UUID: {ha_uuid}")

    enc_ha_config = {
        "encryption_key_primary": encryption_key,
    }

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]

    # --- Worker 1: fire rule with encryption, then go down ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(
        handle_async_messages(reader1, writer1)
    )

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, enc_ha_config)

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task1.cancel()

        my_callback1 = mock.Mock(side_effect=capture_callback)
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule("assignment", my_callback1))

        enable_leader()
        print("worker-1 became a Leader (encryption enabled)")

        # Assert an event that will match the rule
        rs1.assert_event(json.dumps(dict(i=67)))

        # Wait for async task
        try:
            await async_task1
        except asyncio.CancelledError:
            pass

        # Verify the callback was called
        assert my_callback1.called
        assert len(captured_matches) > 0

        matching_uuid = captured_matches[0].matching_uuid
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"
        print(
            f"Rule fired on worker-1! "
            f"matching_uuid: {matching_uuid}"
        )

        # Add action info (persisted with encryption)
        action_data = json.dumps(
            {"action": "print_event", "status": "1"}
        )
        add_action_info(
            ruleset_data["name"], matching_uuid, 0, action_data
        )
        assert action_info_exists(
            ruleset_data["name"], matching_uuid, 0
        )
        print("worker-1 added action info (encrypted)")

        # Verify raw DB columns are encrypted
        raw_event_data = _query_raw_column(
            db_params,
            "SELECT event_data "
            "FROM drools_ansible_matching_event "
            "WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_event_data is not None, (
            "matching_event row should exist"
        )
        assert raw_event_data.startswith("$ENCRYPTED$"), (
            f"event_data should be encrypted, "
            f"got: {raw_event_data[:50]}"
        )
        print(
            f"Verified: event_data is encrypted "
            f"({raw_event_data[:30]}...)"
        )

        raw_session_state = _query_raw_column(
            db_params,
            "SELECT partial_matching_events "
            "FROM drools_ansible_session_state "
            "WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_session_state is not None, (
            "session_state row should exist"
        )
        assert raw_session_state.startswith("$ENCRYPTED$"), (
            f"partial_matching_events should be encrypted, "
            f"got: {raw_session_state[:50]}"
        )
        print(
            f"Verified: session_state is encrypted "
            f"({raw_session_state[:30]}...)"
        )

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate worker-1 JVM down
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: take over with same encryption key ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(
        handle_async_messages(reader2, writer2)
    )

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, enc_ha_config)

        my_callback2 = mock.Mock()
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule("assignment", my_callback2))

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader (encryption enabled)")

        # Wait for recovery of encrypted state
        print("Waiting for recovery...")
        await asyncio.sleep(0.5)

        # Worker-2 should be able to read action info
        # created by worker-1 (encrypted in DB)
        assert action_info_exists(
            ruleset_data["name"], matching_uuid, 0
        )
        retrieved = get_action_info(
            ruleset_data["name"], matching_uuid, 0
        )
        assert retrieved == action_data
        print(
            "worker-2 successfully read encrypted "
            "action info from worker-1"
        )

        # Clean up
        delete_action_info(ruleset_data["name"], matching_uuid)
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass

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
    original_key = _generate_encryption_key()
    new_key = _generate_encryption_key()
    print(f"HA Cluster UUID: {ha_uuid}")

    original_ha_config = {
        "encryption_key_primary": original_key,
    }

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]

    # === Phase 1: Worker-1 as leader with original key ===
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(
        handle_async_messages(reader1, writer1)
    )

    try:
        initialize_ha(
            ha_uuid, "worker-1", db_params, original_ha_config
        )

        captured_matches = []

        def capture_callback(matches):
            captured_matches.append(matches)
            async_task1.cancel()

        my_callback1 = mock.Mock(side_effect=capture_callback)
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule("assignment", my_callback1))

        enable_leader()
        print("worker-1 became a Leader (original key)")

        # Assert an event that will match the rule
        rs1.assert_event(json.dumps(dict(i=67)))

        # Wait for async task
        try:
            await async_task1
        except asyncio.CancelledError:
            pass

        # Verify the callback was called
        assert my_callback1.called
        assert len(captured_matches) > 0

        matching_uuid = captured_matches[0].matching_uuid
        assert (
            matching_uuid is not None
        ), "matching_uuid should be present in HA mode"
        print(
            f"Rule fired on worker-1! "
            f"matching_uuid: {matching_uuid}"
        )

        # Verify raw DB column is encrypted with original key
        raw_event_data = _query_raw_column(
            db_params,
            "SELECT event_data "
            "FROM drools_ansible_matching_event "
            "WHERE ha_uuid = ?",
            ha_uuid,
        )
        assert raw_event_data is not None, (
            "matching_event row should exist"
        )
        assert raw_event_data.startswith("$ENCRYPTED$"), (
            f"event_data should be encrypted, "
            f"got: {raw_event_data[:50]}"
        )
        print(
            f"Verified: event_data is encrypted "
            f"({raw_event_data[:30]}...)"
        )

        # Shut down worker-1 (graceful restart)
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    # Shutdown RulesetCollection to simulate full restart
    if RulesetCollection.engine is not None:
        print("Shutting down RulesetCollection")
        shutdown()
    print("=== Restarting with rotated keys ===")

    # === Phase 2: Restart with rotated keys ===
    rotated_ha_config = {
        "encryption_key_primary": new_key,
        "encryption_key_secondary": original_key,
    }

    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(
        handle_async_messages(reader2, writer2)
    )

    try:
        initialize_ha(
            ha_uuid,
            "worker-1-rotated",
            db_params,
            rotated_ha_config,
        )

        recovered_matches = []

        def recovery_callback(matches):
            recovered_matches.append(matches)

        my_callback2 = mock.Mock(side_effect=recovery_callback)
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule("assignment", my_callback2))

        # Yield to ensure async handler starts reading
        await asyncio.sleep(0.1)

        enable_leader()
        print(
            "worker-1-rotated became a Leader "
            "(new primary + old secondary)"
        )

        # Wait for recovery — should decrypt with secondary key
        print("Waiting for recovery...")
        await asyncio.sleep(1.0)

        # Assert a non-matching event to trigger session state
        # persist, which re-encrypts with the new primary key
        rs2.assert_event(json.dumps(dict(i=1)))
        print("Asserted non-matching event to trigger re-encryption")

        # Give time for persist
        await asyncio.sleep(0.5)

        # Verify session state is re-encrypted with new key
        raw_session_state = _query_raw_column(
            db_params,
            "SELECT partial_matching_events "
            "FROM drools_ansible_session_state "
            "WHERE ha_uuid = ? "
            "AND rule_set_name = ?",
            ha_uuid,
            ruleset_data["name"],
        )
        assert raw_session_state is not None, (
            "session_state row should exist after re-encryption"
        )
        assert raw_session_state.startswith("$ENCRYPTED$"), (
            f"session_state should be encrypted, "
            f"got: {raw_session_state[:50]}"
        )
        print(
            f"Verified: session_state is encrypted "
            f"({raw_session_state[:30]}...)"
        )

        # Decrypt with only the new key (no secondary)
        # to confirm re-encryption used the new primary key
        import jpy

        HAEncryption = jpy.get_type(
            "org.drools.ansible.rulebook.integration"
            ".ha.api.HAEncryption"
        )
        rotated_only = HAEncryption(new_key, None)
        decrypt_result = rotated_only.decrypt(raw_session_state)
        assert not decrypt_result.usedSecondaryKey(), (
            "Should decrypt with primary (new) key, "
            "not secondary"
        )
        assert decrypt_result.plaintext() is not None, (
            "Decrypted session state should not be None"
        )
        print(
            "Verified: session state re-encrypted "
            "with new primary key"
        )

        # Clean up
        disable_leader()
        rs2.end_session()
        print("worker-1-rotated cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass

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

    # --- Worker 1: send partial events ---
    reader1, writer1 = await establish_async_channel()
    async_task1 = asyncio.create_task(
        handle_async_messages(reader1, writer1)
    )

    event_uuid1 = str(uuid.uuid4())
    event_uuid2 = str(uuid.uuid4())

    try:
        initialize_ha(ha_uuid, "worker-1", db_params, {})

        my_callback1 = mock.Mock()
        rs1 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs1.add_rule(Rule(rule_name, my_callback1))

        enable_leader()
        print("worker-1 became a Leader")

        # Send two events satisfying only the first condition
        # (i==0) with explicit UUIDs via meta
        event1 = json.dumps(
            {"meta": {"uuid": event_uuid1}, "i": 0}
        )
        event2 = json.dumps(
            {"meta": {"uuid": event_uuid2}, "i": 0}
        )
        rs1.assert_event(event1)
        rs1.assert_event(event2)
        print("worker-1 sent two partial events")

        await asyncio.sleep(0.5)

        # Rule should not have fired
        assert not my_callback1.called, (
            "Rule should not fire with only one "
            "condition matched"
        )

        # Verify partial event IDs on worker-1
        partial_ids = get_partial_event_ids(
            ruleset_data["name"]
        )
        print(f"Partial event IDs on worker-1: {partial_ids}")
        assert len(partial_ids) == 2
        assert set(partial_ids) == {
            event_uuid1,
            event_uuid2,
        }

        # Worker 1 goes down
        disable_leader()
        rs1.end_session()
        print("worker-1 disabled and session ended")
    finally:
        if not async_task1.done():
            async_task1.cancel()
            try:
                await async_task1
            except asyncio.CancelledError:
                pass

    if RulesetCollection.engine is not None:
        shutdown()
    print("RulesetCollection shut down")

    # --- Worker 2: takes over and verifies partial IDs ---
    reader2, writer2 = await establish_async_channel()
    async_task2 = asyncio.create_task(
        handle_async_messages(reader2, writer2)
    )

    try:
        initialize_ha(ha_uuid, "worker-2", db_params, {})

        my_callback2 = mock.Mock()
        rs2 = Ruleset(
            name=ruleset_data["name"],
            serialized_ruleset=json.dumps(ruleset_data),
            ha_enabled=True,
        )
        rs2.add_rule(Rule(rule_name, my_callback2))

        await asyncio.sleep(0.1)

        enable_leader()
        print("worker-2 became a Leader")

        await asyncio.sleep(0.5)

        # Verify partial event IDs recovered on worker-2
        partial_ids = get_partial_event_ids(
            ruleset_data["name"]
        )
        print(
            f"Partial event IDs on worker-2 "
            f"(after failover): {partial_ids}"
        )
        assert len(partial_ids) == 2
        assert set(partial_ids) == {
            event_uuid1,
            event_uuid2,
        }
        print("Partial event IDs survived failover!")

        # Clean up
        disable_leader()
        rs2.end_session()
        print("worker-2 cleaned up")
    finally:
        if not async_task2.done():
            async_task2.cancel()
            try:
                await async_task2
            except asyncio.CancelledError:
                pass

    # Final cleanup
    if RulesetCollection.engine is not None:
        shutdown()
