import logging
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Rule:
    name: str
    callback: Callable

    def run(self, result: dict):
        self.callback(result)
