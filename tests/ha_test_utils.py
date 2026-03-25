"""Utilities for HA testing to reduce code duplication."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Callable, List, Optional, Tuple
from unittest import mock

import yaml

from drools.dispatch import establish_async_channel, handle_async_messages
from drools.rule import Rule
from drools.ruleset import (
    Ruleset,
    RulesetCollection,
    disable_leader,
    enable_leader,
    shutdown,
)


def load_ast(filename: str) -> dict:
    """Load AST data from a YAML file."""
    test_dir = os.path.dirname(os.path.realpath(__file__))
    with open(f"{test_dir}/{filename}") as f:
        test_data = yaml.safe_load(f)
    return test_data


def create_capture_callback(
    captured_matches: List, cancel_task: Optional[asyncio.Task] = None
) -> Callable:
    """Create a callback that captures matches and optionally cancels a task.

    Args:
        captured_matches: List to append captured matches to
        cancel_task: Optional asyncio task to cancel after first match

    Returns:
        Callback function suitable for use with Rule
    """

    def capture_callback(matches):
        captured_matches.append(matches)
        if cancel_task:
            cancel_task.cancel()

    return capture_callback


async def cancel_async_task(task: asyncio.Task) -> None:
    """Cancel an async task and wait for it to complete.

    Re-raises CancelledError after cancellation as per asyncio best
    practices. Callers should handle this if needed.

    Args:
        task: The asyncio task to cancel
    """
    if task.done():
        # Task already completed/cancelled - nothing to do
        return

    task.cancel()
    await task


async def wait_for_async_processing(duration: float = 0.5) -> None:
    """Wait for async processing to complete.

    Args:
        duration: Duration in seconds to wait (default: 0.5)
    """
    await asyncio.sleep(duration)


async def fire_rule_and_get_matching_uuid(
    rs, my_callback, async_task, captured_matches, event_data=None
):
    """Fire a rule with an event and return the matching_uuid.

    Common pattern used across multiple tests to trigger a rule match
    and extract the matching UUID.

    Args:
        rs: Ruleset instance
        my_callback: Mock callback to verify was called
        async_task: Async task to cancel after match
        captured_matches: List to capture matches
        event_data: Optional event data (default: {"i": 67})

    Returns:
        str: The matching_uuid from the captured match
    """
    # Assert an event to generate a match with matching_uuid
    if event_data is None:
        event_data = {"i": 67}
    rs.assert_event(json.dumps(event_data))

    # Wait for async task to complete
    try:
        await cancel_async_task(async_task)
    except asyncio.CancelledError:  # NOSONAR
        pass  # Expected when cancelling

    # Verify callback was called
    assert_rule_fired(my_callback, captured_matches)

    # Extract matching_uuid from the Matches object
    matching_uuid = captured_matches[0].matching_uuid

    # Ensure we have a matching_uuid (should be present in HA mode)
    assert (
        matching_uuid is not None
    ), "matching_uuid should be present in HA mode"

    return matching_uuid


async def verify_grace_period_recovery(
    rs2,
    my_callback2,
    captured_matches,
    ruleset_data,
    advance_time_seconds=15,
    additional_advance_seconds=5,
):
    """Test grace period recovery pattern with clock advancement.

    Common pattern for grace period tests where worker-2 takes over
    after time has advanced past the original window expiry.

    Args:
        rs2: Ruleset instance for worker-2
        my_callback2: Mock callback for worker-2
        captured_matches: List to capture matches
        ruleset_data: Ruleset data for cleanup
        advance_time_seconds: Time to advance before enabling leader
        additional_advance_seconds: Additional time to advance after

    Returns:
        str: The matching_uuid from the recovered match
    """
    from drools.ruleset import delete_action_info, enable_leader

    # Advance Node2 clock BEFORE becoming leader
    rs2.advance_time(advance_time_seconds, "Seconds")

    # Yield to ensure async handler starts reading
    await wait_for_async_processing(0.1)

    # Node2 becomes leader - recovery should dispatch the match
    enable_leader()
    print("worker-2 became a Leader")

    # Wait for recovery and async delivery
    print("Waiting for recovery...")
    await wait_for_async_processing(1.0)

    # The match should have been dispatched
    assert_rule_fired(
        my_callback2,
        captured_matches,
        "Grace period match should be dispatched via async channel",
    )
    matching_uuid = captured_matches[0].matching_uuid
    print(f"Rule fired on worker-2! matching_uuid: {matching_uuid}")

    # After recovery, advancing time should NOT produce a duplicate match
    initial_count = len(captured_matches)
    rs2.advance_time(additional_advance_seconds, "Seconds")
    await wait_for_async_processing(0.5)
    assert (
        len(captured_matches) == initial_count
    ), "No duplicate match should fire after grace period recovery"
    print("Confirmed: no duplicate match")

    # Clean up
    if matching_uuid:
        delete_action_info(ruleset_data["name"], matching_uuid)
    print("worker-2 cleaned up")

    return matching_uuid


def assert_rule_fired(
    callback: mock.Mock, captured_matches: List, message: str = ""
) -> None:
    """Assert that a rule fired successfully.

    Args:
        callback: The mock callback that should have been called
        captured_matches: List of captured matches
        message: Optional custom assertion message
    """
    msg = message or "Rule should have fired"
    assert callback.called, msg
    assert len(captured_matches) > 0, "Should have captured matches"


def assert_rule_not_fired(
    callback: mock.Mock, captured_matches: List, message: str = ""
) -> None:
    """Assert that a rule did not fire.

    Args:
        callback: The mock callback that should not have been called
        captured_matches: List of captured matches
        message: Optional custom assertion message
    """
    msg = message or "Rule should not have fired"
    assert not callback.called, msg
    assert len(captured_matches) == 0, "Should not have captured matches"


def create_ruleset_with_callback(
    ruleset_data: dict,
    rule_name: str,
    captured_matches: List,
    cancel_task: Optional[asyncio.Task] = None,
) -> Tuple[Ruleset, mock.Mock]:
    """Create a ruleset with a standard capture callback.

    Args:
        ruleset_data: The ruleset definition from AST
        rule_name: Name of the rule to add
        captured_matches: List to capture matches
        cancel_task: Optional task to cancel on first match

    Returns:
        Tuple of (Ruleset, mock.Mock callback)
    """
    callback_fn = create_capture_callback(captured_matches, cancel_task)
    my_callback = mock.Mock(side_effect=callback_fn)

    rs = Ruleset(
        name=ruleset_data["name"],
        serialized_ruleset=json.dumps(ruleset_data),
        ha_enabled=True,
    )
    rs.add_rule(Rule(rule_name, my_callback))

    return rs, my_callback


@asynccontextmanager
async def async_channel_manager():
    """Context manager for async channel lifecycle.

    Yields:
        Tuple of (reader, writer, async_task)

    Example:
        async with async_channel_manager() as (reader, writer, async_task):
            # Use the channel
            pass
        # Channel is automatically cleaned up
    """
    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    try:
        yield reader, writer, async_task
    finally:
        # Cancel and await the task
        if not async_task.done():
            async_task.cancel()
            try:
                await async_task
            except asyncio.CancelledError:  # NOSONAR
                # Safe to suppress: we initiated this cancellation during
                # cleanup, not from external cancellation
                pass


@asynccontextmanager
async def ha_worker_manager(
    ha_uuid: str,
    worker_name: str,
    db_params: dict,
    ha_config: Optional[dict] = None,
    ruleset_data: Optional[dict] = None,
    rule_name: Optional[str] = None,
    captured_matches: Optional[List] = None,
    auto_enable_leader: bool = True,
    shutdown_on_exit: bool = False,
):
    """Context manager for HA worker lifecycle.

    Manages:
    - Async channel setup/teardown
    - HA initialization
    - Optional ruleset creation
    - Leader enable/disable
    - Session cleanup
    - Optional shutdown

    Args:
        ha_uuid: HA cluster UUID
        worker_name: Worker instance name
        db_params: Database connection parameters
        ha_config: Optional HA configuration
        ruleset_data: Optional ruleset definition from AST
        rule_name: Optional rule name (required if ruleset_data
            provided)
        captured_matches: Optional list to capture matches (required
            if ruleset_data provided)
        auto_enable_leader: Whether to enable leader automatically
            (default: True)
        shutdown_on_exit: Whether to shutdown RulesetCollection on
            exit (default: False)

    Yields:
        If ruleset_data provided: dict with keys 'ruleset',
            'callback', 'async_task'
        Otherwise: dict with key 'async_task'

    Example:
        async with ha_worker_manager(
            ha_uuid, "worker-1", db_params, {},
            ruleset_data, "r1", captured_matches
        ) as ctx:
            rs = ctx['ruleset']
            rs.assert_event(json.dumps({"i": 42}))
    """
    from drools.ruleset import initialize_ha

    reader, writer = await establish_async_channel()
    async_task = asyncio.create_task(handle_async_messages(reader, writer))

    config = ha_config if ha_config is not None else {}

    try:
        # Initialize HA
        initialize_ha(ha_uuid, worker_name, db_params, config)

        # Create ruleset if requested
        rs = None
        callback = None
        if ruleset_data and rule_name and captured_matches is not None:
            rs, callback = create_ruleset_with_callback(
                ruleset_data, rule_name, captured_matches, None
            )

        # Enable leader if requested
        if auto_enable_leader:
            enable_leader()

        # Yield context
        if rs and callback:
            yield {
                "ruleset": rs,
                "callback": callback,
                "async_task": async_task,
            }
        else:
            yield {"async_task": async_task}

    finally:
        # Cleanup
        try:
            disable_leader()
        except Exception:
            # May already be disabled
            pass

        if rs:
            try:
                rs.end_session()
            except Exception:
                pass

        # Cancel and await the task
        if not async_task.done():
            async_task.cancel()
        # Always await to ensure cleanup, even if already cancelled
        try:
            await async_task
        except asyncio.CancelledError:  # NOSONAR
            # Safe to suppress: we initiated this cancellation during
            # cleanup, not from external cancellation
            pass

        if shutdown_on_exit and RulesetCollection.engine is not None:
            shutdown()


def perform_failover_shutdown() -> None:
    """Perform shutdown between failover scenarios.

    Shuts down RulesetCollection to simulate worker JVM going down.
    """
    if RulesetCollection.engine is not None:
        shutdown()


def generate_encryption_key() -> str:
    """Generate a random 256-bit AES key, base64-encoded."""
    import base64

    key_bytes = os.urandom(32)
    return base64.b64encode(key_bytes).decode("ascii")


def query_raw_column(
    db_params: dict, sql: str, *param_values
) -> Optional[str]:
    """Query a raw column value from DB via JDBC through JPY.

    Bypasses the state manager's decryption so we can verify
    data is actually encrypted at rest.

    Args:
        db_params: Database connection parameters
        sql: SQL query with ? placeholders
        param_values: Values for placeholders

    Returns:
        The column value as a string, or None if no row found
    """
    import jpy

    driver_manager = jpy.get_type("java.sql.DriverManager")

    db_type = db_params.get("db_type", "h2")
    if db_type == "h2":
        db_file = db_params.get("db_file_path", "./eda_ha")
        jdbc_url = f"jdbc:h2:file:{db_file};MODE=PostgreSQL"
        conn = driver_manager.getConnection(jdbc_url, "SA", "")
    else:
        host = db_params.get("host", "localhost")
        port = db_params.get("port", 5432)
        database = db_params.get("database", "eda_ha_db")
        user = db_params.get("user", "eda_user")
        password = db_params.get("password", "eda_password")
        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        conn = driver_manager.getConnection(jdbc_url, user, password)

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
