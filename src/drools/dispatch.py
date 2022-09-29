from dataclasses import dataclass

from .ruleset import RulesetCollection


@dataclass(frozen=True)
class Dispatch:
    session_id: int
    serialized_result: str

    def run(self) -> None:
        rs = RulesetCollection.get_by_session_id(self.session_id)
        rs.dispatch(self.serialized_result)
