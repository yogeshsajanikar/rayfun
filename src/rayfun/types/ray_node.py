# Copyright 2023 Yogesh Sajanikar
from __future__ import annotations

from abc import abstractmethod
from typing import TypeVar, Any, Callable, overload, cast

import ray
from ray import ObjectRef
from ray.dag import FunctionNode
from ray.remote_function import RemoteFunction
from returns.interfaces.applicative import Applicative1
from returns.interfaces.container import Container1
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import SupportsKind1, Kind1, dekind

from rayfun.utils import Binder

_T = TypeVar("_T")
_T0 = TypeVar("_T0")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")

_RayNodeType = TypeVar("_RayNodeType", bound="RayNode")


class RayNode(BaseContainer, SupportsKind1["RayNode", _T]):
    """
    The base class for all RayNode types.
    """

    def __init__(self, value: Any):
        super().__init__(value)

    @abstractmethod
    def execute(self) -> "RayNode[_T]":
        """
        Execute the node.
        """
        ...

    @abstractmethod
    def reduce(self) -> RayNode[_T]:
        """
        Reduce the node.
        """
        ...

    @property
    @abstractmethod
    def wrapped(self) -> Any:
        """
        Returns the wrapped value.
        """
        ...


class RayObjectNode(RayNode[_T]):
    """
    The base class for all RayObjectNode types.
    """

    def __init__(self, value: ObjectRef):
        super().__init__(value)

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        return self

    def reduce(self) -> RayNode[_T]:
        return self

    # def __eq__(self, other):
    #     if not isinstance(other, RayNode):
    #         return False
    #     reduced_other = other.reduce()
    #     return self._inner_value == reduced_other._inner_value or ray.get(
    #         self._inner_value
    #     ) == ray.get(reduced_other._inner_value)

    @property
    def wrapped(self) -> ObjectRef:
        """
        Returns the wrapped value.
        """
        return self._inner_value


class RayFinalFunctionNode(RayNode[_T]):
    def __init__(self, value: FunctionNode):
        super().__init__(value)

    # def __eq__(self, other):
    #     reduced_other = other.reduce()
    #     reduced_self = self.reduce()
    #     return reduced_other == reduced_self

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        ref: ObjectRef = self._inner_value.execute()
        return RayObjectNode[_T](ref)

    def reduce(self) -> RayNode[_T]:
        return self.execute()

    @property
    def wrapped(self) -> FunctionNode:
        """
        Returns the wrapped value.
        """
        return self._inner_value


_RayFunctionNode = TypeVar("_RayFunctionNode", bound="RayFunctionNode")


class RayFunctionNode(RayNode[_T]):
    """
    The base class for all RayFunctionNode types.
    """

    @classmethod
    def from_remote_function(cls, value: RemoteFunction) -> "RayFunctionNode[_T]":
        """
        Create a function node from a remote function.
        """
        binder = Binder.from_function(value.remote)
        return RayFunctionNode(value, binder)

    @property
    def inner_func(self):
        return self._inner_value[0]

    @property
    def binder(self):
        return self._inner_value[1]

    def __init__(self, value: RemoteFunction, binder: Binder):
        super().__init__((value, binder))

    # def __eq__(self, other):
    #     reduced_other = other.reduce()
    #     reduced_self = self.reduce()
    #     return reduced_other == reduced_self

    @property
    def wrapped(self) -> RemoteFunction:
        """
        Returns the wrapped value.
        """
        return self._inner_value[0]

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        if self.binder.callable():
            # Get the arguments from the binder
            args = self.binder.args
            func_node: RemoteFunction = self.inner_func
            # bind the arguments to the function
            node = func_node.bind(*args)
            final_node: RayNode[_T] = RayFinalFunctionNode(node)
            return final_node.execute()

        # If the function is not callable, raise an error
        return self

    def reduce(self) -> RayNode[_T]:
        if self.binder.callable():
            func_node = self.execute()
            return func_node.reduce()

        raise RayContextError("The function is not callable, and cannot be reduced")

    # Till there are parameters to be bound, the function is not callable, and will return another `RayFunctionNode`.
    # Once all the parameters are bound, the function is callable, and will return a `RayExeFunctionNode`.

    @overload
    def apply_arg(
        self: "RayFunctionNode[Callable[[_T0], _T1]]", arg: RayNode[_T0]
    ) -> "RayFunctionNode[Callable[[], _T1]]":
        ...

    @overload
    def apply_arg(
        self: "RayFunctionNode[Callable[[_T0, _T1], _T2]]", arg: RayNode[_T0]
    ) -> "RayFunctionNode[Callable[[_T1], _T2]]":
        ...

    @overload
    def apply_arg(
        self: "RayFunctionNode[Callable[[_T0, _T1, _T2], _T3]]", arg: RayNode[_T1]
    ) -> "RayFunctionNode[Callable[[_T1, _T2], _T3]]":
        ...

    def apply_arg(self, arg):
        # check if the function is callable
        if not self.binder.bindable():
            # If the function is callable, raise an error
            raise TypeError("The function is callable")

        # bind the argument to the function
        new_binder = self.binder.bind(arg.wrapped)
        # create a new function node
        return RayFunctionNode(self.inner_func, new_binder)


_U = TypeVar("_U")


class RayContextError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class RayContext(BaseContainer, SupportsKind1["RayContext", _T], Container1[_T]):
    """
    The base class for all RayContext types.
    """

    @property
    def wrapped(self) -> RayNode[_T]:
        """
        Returns the wrapped value.
        """
        return self._inner_value

    @classmethod
    def from_value(cls, value: _U) -> "RayContext[_U]":
        """
        Create a context from a value.
        """
        if isinstance(value, RayNode):
            return RayContext[_U](value)

        # check if the value is a callable
        if callable(value):
            remoted: RemoteFunction = ray.remote(value)  # type: ignore
            function_node = RayFunctionNode[_U].from_remote_function(remoted)
            return RayContext(function_node)

        remote_value = ray.put(value)
        ref_node = RayObjectNode[_U](remote_value)
        return RayContext(ref_node)

    def __init__(self, value: RayNode[_T]):
        super().__init__(value)

    # def __eq__(self, other):
    #     if not isinstance(other, RayContext):
    #         return False
    #     return self._inner_value == other._inner_value

    @overload
    def apply(
        self, container: Kind1["RayContext", Callable[[_T], _U]]
    ) -> "RayContext[_U]":
        """
        Allows to apply a wrapped function over a container.
        """
        ...

    @overload
    def apply(
        self, container: Kind1["RayContext", Callable[[_T0, _T1], _U]]
    ) -> "RayContext[Callable[[_T1], _U]]":
        ...

    def apply(self, container):
        # Check if wrapped value of the container is a subclass of RayFunctionNode
        if not isinstance(container.wrapped, RayFunctionNode):
            raise RayContextError(
                "A RayFunctionNode is expected for applying to a RayContext"
            )

        # Get the wrapped value of the container
        self_wrap = self.wrapped
        func_node = cast(RayFunctionNode, container.wrapped)
        applied_func = func_node.apply_arg(self_wrap)
        return RayContext(applied_func.execute())

    def map(self, function: Callable[[_T], _U]) -> "RayContext[_U]":
        """
        Maps a function over a container.
        """
        # check if the value is a callable
        f_app = RayContext.from_value(function)
        return self.apply(f_app)

    def bind(
        self, function: Callable[[_T], Kind1["RayContext", _U]]
    ) -> "RayContext[_U]":
        """
        Binds a function over a container.

        `bind` gives a stronger sequencing than `apply`. It forces the `self` to be evaluated before passing
        it to the function.
        """
        # check if the value is a callable
        reduced = self.wrapped.reduce()
        executed = reduced.execute()
        unwrapped: _T = cast(_T, ray.get(executed.wrapped))
        return dekind(function(unwrapped))

    def run(self) -> RayNode[_T]:
        """
        Runs the computation.
        """
        reduced = self.wrapped.reduce()
        return reduced.execute()

    def plot(self, file_name: str) -> None:
        """
        Plots the computation.
        """
        ray.dag.vis_utils.plot(self.wrapped.wrapped, file_name)


def flatten(context: RayContext[RayContext[_U]]) -> RayContext[_U]:
    """
    Flatten a nested context.

    :param context: Nested context
    :return: Flattened context
    """
    first_run: RayNode[RayContext[_U]] = context.run()
    second_run: RayContext[_U] = ray.get(first_run.wrapped)
    return second_run


def ray_context_conditional(
    cond: RayContext[bool],
    true_st: Callable[[], RayContext[_U]],
    false_st: Callable[[], RayContext[_U]],
) -> RayContext[_U]:
    """
    Conditional execution

    Validates the condition, and executes the true or false statement based on the condition.

    :param cond: Condition
    :param true_st: True statement
    :param false_st: False statement
    :return: Conditional execution

    """

    def _conditional_internal(flag: bool) -> RayContext[_U]:
        if flag:
            return true_st()
        return false_st()

    ray_conditional = RayContext.from_value(_conditional_internal)

    result: RayContext[RayContext[_U]] = cond.apply(ray_conditional)
    return flatten(result)


@overload
def ray_context_apply(
    func: RayContext[Callable[[_T0], _U]], arg1: RayContext[_T0]
) -> RayContext[_U]:
    ...


@overload
def ray_context_apply(
    func: RayContext[Callable[[_T0, _T1], _U]],
    arg1: RayContext[_T0],
    arg2: RayContext[_T1],
) -> RayContext[_U]:
    ...


@overload
def ray_context_apply(
    func: RayContext[Callable[[_T0, _T1, _T2], _U]],
    arg1: RayContext[_T0],
    arg2: RayContext[_T1],
    arg3: RayContext[_T2],
) -> RayContext[_U]:
    ...


@overload
def ray_context_apply(
    func: RayContext[Callable[[_T0, _T1, _T2, _T3], _U]],
    arg1: RayContext[_T0],
    arg2: RayContext[_T1],
    arg3: RayContext[_T2],
    arg4: RayContext[_T3],
) -> RayContext[_U]:
    ...


def ray_context_apply(func, *args):
    """
    Applies a function to a list of arguments.

    :param func: Function to apply
    :param args: List of arguments
    :return: Applied function
    """
    a_func = func
    for arg in args:
        a_func = arg.apply(a_func)

    return a_func
