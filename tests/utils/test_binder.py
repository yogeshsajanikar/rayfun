# Copyright 2023 Yogesh Sajanikar

from rayfun.utils import Binder


def function_no_args():
    return 1


def test_function_no_args():
    binder = Binder.from_function(function_no_args)
    assert binder.callable() is True
    assert binder.func() is 1


def function_one_arg(arg):
    return arg


def test_function_one_arg():
    binder = Binder.from_function(function_one_arg)
    assert binder.callable() is False
    binder = binder.bind(1)
    assert binder.callable() is True
    assert binder.func() == 1
