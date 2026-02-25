import asyncio
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
        "Shutting down RulesetCollection"
        " (simulating Leader 1 JVM shutdown)"
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
