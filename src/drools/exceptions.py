class MessageNotHandledException(Exception):
    pass


class MessageObservedException(Exception):
    pass


class RulesetNotFoundError(Exception):
    pass


class RuleNotFoundError(Exception):
    pass


class InvalidRuleMissingConditionError(Exception):
    pass


class InvalidRuleError(Exception):
    pass
