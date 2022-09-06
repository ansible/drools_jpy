from dataclasses import dataclass


@dataclass(frozen=True)
class Condition:
    var_name: str
    expression: str
