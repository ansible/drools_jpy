import copy
from dataclasses import dataclass
from distutils.util import strtobool

from jinja2 import BaseLoader, Environment

from .condition import Condition
from .exceptions import InvalidRuleError, InvalidRuleMissingConditionError


def _parse_condition(s: str):
    _s = (
        s.replace("event.", "")
        .replace("events.", "")
        .replace("fact.", "")
        .replace("facts.", "")
    )
    z = _s.split("<<")
    if len(z) == 2:
        return z[0].strip(), "{{ " + z[1] + " }}"
    return None, "{{ " + _s + " }}"


@dataclass
class RuleProcessor:
    data: dict
    index: int
    rule_name: str = None
    _all_match: bool = True

    def __post_init__(self):
        if "condition" not in self.data:
            raise InvalidRuleMissingConditionError(
                "Rule should contain a condition"
            )
        self._cached_results = {}

        if "name" in self.data:
            self.rule_name = self.data["name"]
        else:
            self.rule_name = f"r_{self.index}"

        tests = self.data["condition"]

        self.conditions = []
        if isinstance(tests, str):
            tests = [tests]
        elif isinstance(tests, dict):
            if "all" in tests:
                tests = tests["all"]
            elif "any" in tests:
                tests = tests["any"]
                self._all_match = False

        if not isinstance(tests, list):
            InvalidRuleError("Rule should have a list of conditions or tests")

        for test in tests:
            var_name, cond = _parse_condition(test)
            self.conditions.append(
                Condition(var_name=var_name, expression=cond)
            )

    def process(self, context: dict, event: dict):
        current_context = {**context, **event}
        results = {}
        index = 0
        for condition in self.conditions:
            rtemplate = Environment(loader=BaseLoader).from_string(
                condition.expression
            )
            if bool(strtobool(rtemplate.render(**current_context))):
                if condition.var_name is None:
                    results["m"] = event
                    self._cached_results["m"] = event
                else:
                    results[condition.var_name] = event
                    self._cached_results[condition.var_name] = event

                index += 1

        if self._all_match:
            if len(self._cached_results.keys()) == len(self.conditions):
                results = copy.deepcopy(self._cached_results)
                self._cached_results = {}
            else:
                results = {}

        return results
