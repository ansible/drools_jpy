import asyncio
import json
import os
import uuid
from unittest import mock

import pytest
import yaml

from drools.dispatch import establish_async_channel, handle_async_messages
from drools.rule import Rule
from drools.ruleset import (
    Ruleset,
    initialize_ha,
    enable_leader,
    disable_leader,
    get_ha_stats,
    add_action_info,
    get_action_info,
    get_action_status,
    action_info_exists,
    delete_action_info,
)


def load_ast(filename: str) -> dict:
    test_dir = os.path.dirname(os.path.realpath(__file__))
    with open(f"{test_dir}/{filename}") as f:
        test_data = yaml.safe_load(f)
    return test_data


@pytest.fixture
def postgres_params():
    """PostgreSQL connection parameters for testing"""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "drools_ha_test"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


@pytest.fixture
def ha_config():
    """HA configuration parameters"""
    return {
        "syncInterval": 1000,  # milliseconds
        "stateCheckInterval": 500,  # milliseconds
    }


@pytest.mark.asyncio
async def test_ha_initialization_and_leader_lifecycle(postgres_params, ha_config):
    """Test HA initialization, createRuleset, enableLeader, and assertEvent"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        # 1. Initialize HA with a unique UUID
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, postgres_params, ha_config)

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
            ha_enabled=True
        )
        rs.add_rule(Rule("assignment", my_callback))

        # 3. Enable leader mode
        leader_name = "test-leader-1"
        enable_leader(leader_name)

        # Verify HA stats after enabling leader
        ha_stats = get_ha_stats()
        assert ha_stats is not None
        assert isinstance(ha_stats, dict)

        # 4. Assert an event that will match the rule
        rs.assert_event(json.dumps(dict(i=67)))

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
            action_data = json.dumps({"action": "print_event", "status": "pending"})

            # Test adding action info
            add_action_info(ruleset_data["name"], matching_uuid, 0, action_data)

            # Verify action exists
            assert action_info_exists(ruleset_data["name"], matching_uuid, 0)

            # Get action info
            retrieved_action = get_action_info(ruleset_data["name"], matching_uuid, 0)
            assert retrieved_action == action_data

            # Clean up action info
            delete_action_info(ruleset_data["name"], matching_uuid)

        # 6. Disable leader mode
        disable_leader(leader_name)

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
async def test_action_info_lifecycle(postgres_params):
    """Test ActionInfo CRUD operations"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        # Initialize HA
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, postgres_params, {})

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
            ha_enabled=True
        )
        rs.add_rule(Rule("assignment", my_callback))

        # Enable leader
        enable_leader("action-test-leader")

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
        assert matching_uuid is not None, "matching_uuid should be present in HA mode"

        # Test ActionInfo operations
        action_index = 0
        action_data = json.dumps({"action": "print_event", "status": "pending"})

        # 1. Check action doesn't exist initially
        assert not action_info_exists(ruleset_data["name"], matching_uuid, action_index)

        # 2. Add action info
        add_action_info(ruleset_data["name"], matching_uuid, action_index, action_data)

        # 3. Verify action exists
        assert action_info_exists(ruleset_data["name"], matching_uuid, action_index)

        # 4. Get action info
        retrieved_action = get_action_info(ruleset_data["name"], matching_uuid, action_index)
        assert retrieved_action == action_data

        # 5. Get action status
        status = get_action_status(ruleset_data["name"], matching_uuid, action_index)
        assert status is not None

        # 6. Delete action info
        delete_action_info(ruleset_data["name"], matching_uuid)

        # 7. Verify action no longer exists
        assert not action_info_exists(ruleset_data["name"], matching_uuid, action_index)

        # Clean up
        disable_leader("action-test-leader")
        rs.end_session()
    finally:
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_ha_stats(postgres_params):
    """Test getting HA statistics"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, postgres_params, {})

        # Get stats before enabling leader
        stats_before = get_ha_stats()
        assert isinstance(stats_before, dict)

        # Enable leader
        enable_leader("stats-test-leader")

        # Get stats after enabling leader
        stats_after = get_ha_stats()
        assert isinstance(stats_after, dict)

        # Clean up
        disable_leader("stats-test-leader")
    finally:
        async_task.cancel()
        try:
            await async_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_multiple_action_infos(postgres_params):
    """Test managing multiple actions for the same matching event"""

    # Establish async channel for HA communication
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        instance_uuid = str(uuid.uuid4())
        initialize_ha(instance_uuid, postgres_params, {})

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
            ha_enabled=True
        )
        rs.add_rule(Rule("assignment", my_callback))

        enable_leader("multi-action-leader")

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
        assert matching_uuid is not None, "matching_uuid should be present in HA mode"

        # Add multiple actions
        for i in range(3):
            action_data = json.dumps({"action": f"action_{i}", "index": i})
            add_action_info(ruleset_data["name"], matching_uuid, i, action_data)

        # Verify all actions exist
        for i in range(3):
            assert action_info_exists(ruleset_data["name"], matching_uuid, i)
            action = get_action_info(ruleset_data["name"], matching_uuid, i)
            assert f"action_{i}" in action

        # Delete all actions at once
        delete_action_info(ruleset_data["name"], matching_uuid)

        # Verify all are deleted
        for i in range(3):
            assert not action_info_exists(ruleset_data["name"], matching_uuid, i)

        disable_leader("multi-action-leader")
        rs.end_session()
    finally:
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:
                pass


@pytest.mark.skip(reason="Requires PostgreSQL database setup")
def test_ha_failover_scenario(postgres_params):
    """Test HA failover scenario with leader switch"""

    # Leader 1 starts
    uuid1 = str(uuid.uuid4())
    initialize_ha(uuid1, postgres_params, {})

    test_data = load_ast("asts/rules_with_assignment.yml")
    ruleset_data = test_data[0]["RuleSet"]

    rs1 = Ruleset(
        name=ruleset_data["name"],
        serialized_ruleset=json.dumps(ruleset_data),
        ha_enabled=True
    )
    rs1.add_rule(Rule("assignment", mock.Mock()))

    enable_leader("leader-1")

    # Leader 1 creates some action info
    matching_uuid = str(uuid.uuid4())
    add_action_info(ruleset_data["name"], matching_uuid, 0,
                   json.dumps({"action": "test", "status": "pending"}))

    # Leader 1 goes down
    disable_leader("leader-1")
    rs1.end_session()

    # Leader 2 starts (would recover state from database)
    uuid2 = str(uuid.uuid4())
    initialize_ha(uuid2, postgres_params, {})

    rs2 = Ruleset(
        name=ruleset_data["name"],
        serialized_ruleset=json.dumps(ruleset_data),
        ha_enabled=True
    )
    rs2.add_rule(Rule("assignment", mock.Mock()))

    enable_leader("leader-2")

    # Leader 2 should be able to read the action info created by Leader 1
    assert action_info_exists(ruleset_data["name"], matching_uuid, 0)

    # Clean up
    delete_action_info(ruleset_data["name"], matching_uuid)
    disable_leader("leader-2")
    rs2.end_session()
