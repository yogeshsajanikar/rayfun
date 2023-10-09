# Copyright 2023 Yogesh Sajanikar

from rayfun.utils import Binder


def function_no_args():
    return 1


def test_function_no_args():
    binder = Binder.from_function(function_no_args)
    assert binder.callable() is True
    assert binder.bindable() is False
    assert binder.func() is 1


def function_one_arg(arg):
    return arg


def test_function_one_arg():
    binder = Binder.from_function(function_one_arg)
    assert binder.callable() is False
    assert binder.bindable() is True
    binder = binder.bind(1)
    assert binder.callable() is True
    assert binder.bindable() is False
    assert binder.func() == 1


def function_with_variable_arguments(*args):
    return None


def test_function_with_variable_arguments():
    binder = Binder.from_function(function_with_variable_arguments)
    assert binder.func is not None
    assert binder.callable() is True
    assert binder.bindable() is True
