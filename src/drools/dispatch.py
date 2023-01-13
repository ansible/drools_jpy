import asyncio
import json
import logging
from dataclasses import dataclass

from .ruleset import RulesetCollection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dispatch:
    session_id: int
    serialized_result: str

    def run(self) -> None:
        logger.debug("Dispatching for session " + str(self.session_id))
        rs = RulesetCollection.get_by_session_id(self.session_id)
        rs.dispatch(self.serialized_result)


async def establish_async_channel():
    logger.debug("Establishing async channel")
    return await asyncio.open_connection(
        "localhost", RulesetCollection.response_port()
    )


async def handle_async_messages(reader, writer):
    try:
        while True:
            length = await reader.read(4)
            bytes_to_read = int.from_bytes(length, "big")
            logger.debug(
                "Reading " + str(bytes_to_read) + " from async channel"
            )
            payload = await reader.read(bytes_to_read)
            if payload:
                logger.debug("Async Response " + str(payload))
                data = json.loads(payload)
                for result in data["result"]:
                    dispatch = Dispatch(
                        session_id=data["session_id"],
                        serialized_result=result,
                    )
                    dispatch.run()
    except asyncio.CancelledError:
        logger.debug("Shutting down async channel")
        RulesetCollection.shutdown()
    finally:
        writer.close()
