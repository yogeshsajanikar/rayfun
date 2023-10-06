# Copyright 2023 Yogesh Sajanikar

from rayfun.utils import Binder


def function_no_args():
    return 1


def test_function_no_args():
    binder = Binder.from_function(function_no_args)
    assert binder.callable() is True
