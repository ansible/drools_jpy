from unittest import mock

from drools.rule import Rule


def test_rule():
    my_callback = mock.Mock()
    obj = Rule("fred", my_callback)
    obj.run(dict(a=1))
    my_callback.assert_called_with(dict(a=1))
