import asyncio
import json
from dataclasses import dataclass

from .ruleset import RulesetCollection


@dataclass(frozen=True)
class Dispatch:
    session_id: int
    serialized_result: str

    def run(self) -> None:
        rs = RulesetCollection.get_by_session_id(self.session_id)
        rs.dispatch(self.serialized_result)


async def establish_async_channel():
    return await asyncio.open_connection(
        "localhost", RulesetCollection.response_port()
    )


async def handle_async_messages(reader, writer):
    try:
        while True:
            length = await reader.read(4)
            bytes_to_read = int.from_bytes(length, "big")
            payload = await reader.read(bytes_to_read)
            if payload:
                data = json.loads(payload)
                for result in data["result"]:
                    dispatch = Dispatch(
                        session_id=data["session_id"],
                        serialized_result=result,
                    )
                    dispatch.run()
    except asyncio.CancelledError:
        RulesetCollection.shutdown()
    finally:
        writer.close()
