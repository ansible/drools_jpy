import json
from dataclasses import dataclass, field
from typing import List

from .rule_processor import RuleProcessor


@dataclass
class RulesetEvaluator:
    serialized_rules: str
    context: dict
    _rule_processors: List[RuleProcessor] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self):
        rules = json.loads(self.serialized_rules)

        index = 0
        for rule in rules:
            self._rule_processors.append(RuleProcessor(index=index, data=rule))
            index += 1

    def get_facts(self):
        return self.context

    def retract_fact(self, fact: dict):
        for key, _ in fact.items():

            if key in self.context:
                self.context.pop(key)

        return self.process(fact)

    def assert_fact(self, fact: dict):
        self.context = {**self.context, **fact}
        return self.process(fact)

    def process(self, event: dict):
        for rp in self._rule_processors:
            result = rp.process(self.context, event)
            if result:
                yield {rp.rule_name: result}

        return None
