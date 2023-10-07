# Copyright 2023 Yogesh Sajanikar

from functools import partial
from typing import Any
from inspect import signature


class Binder:
    """
    A helper class to bind a function to its arguments.
    """

    _bound: partial

    def __init__(self, value: partial):
        self._bound = value

    @classmethod
    def from_function(cls, func) -> "Binder":
        """
        Create a binder from a function.
        """
        return cls(partial(func))

    def bind(self, arg: Any) -> "Binder":
        """
        Bind an argument to the function.
        """
        bound = partial(self._bound, arg)
        return Binder(bound)

    @property
    def args(self) -> tuple:
        """
        Get the arguments of the binder.
        """
        return self._bound.args

    @property
    def func(self) -> partial:
        """
        Get the function of the binder.
        """
        return self._bound

    def callable(self) -> bool:
        """
        Check if the binder is complete and callable
        """
        sig = signature(self.func)
        # If signature has no parameters, it is callable
        return len(sig.parameters) <= 0
