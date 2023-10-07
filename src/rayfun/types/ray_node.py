# Copyright 2023 Yogesh Sajanikar
from abc import abstractmethod
from typing import TypeVar, Any, Callable, overload

import ray
from ray import ObjectRef
from ray.dag import FunctionNode
from ray.remote_function import RemoteFunction
from returns.interfaces.applicative import Applicative1
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import SupportsKind1, Kind1

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


class RayObjectNode(RayNode[_T]):
    """
    The base class for all RayObjectNode types.
    """

    def __init__(self, value: ObjectRef):
        super().__init__(value)
        self._inner_value = value

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        return self


class RayFinalFunctionNode(RayNode[_T]):
    def __init__(self, value: FunctionNode):
        super().__init__(value)
        self._inner_value = value

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        ref: ObjectRef = self._inner_value.execute()
        return RayObjectNode[_T](ref)


_RayFunctionNode = TypeVar("_RayFunctionNode", bound="RayFunctionNode")


class RayFunctionNode(RayNode[_T]):
    """
    The base class for all RayFunctionNode types.
    """

    _binder: Binder

    @classmethod
    def from_remote_function(cls, value: RemoteFunction) -> "RayFunctionNode[_T]":
        """
        Create a function node from a remote function.
        """
        binder = Binder.from_function(value.remote)
        return RayFunctionNode(value, binder)

    def __init__(self, value: RemoteFunction, binder: Binder):
        super().__init__(value)
        self._binder = binder

    def execute(self) -> RayNode[_T]:
        """
        Execute the node.
        """
        if self._binder.callable():
            # Get the arguments from the binder
            args = self._binder.args
            func_node: RemoteFunction = self._inner_value
            # bind the arguments to the function
            node = func_node.bind(*args)
            # execute the function
            ref: ObjectRef = node.execute()
            # return the result as RayObjectNode
            return RayObjectNode(ref)

        # If the function is not callable, raise an error
        raise TypeError("The function is not callable")

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
        if self._binder.callable():
            # If the function is callable, raise an error
            raise TypeError("The function is callable")

        # bind the argument to the function
        new_binder = self._binder.bind(arg._inner_value)
        # create a new function node
        return RayFunctionNode(self._inner_value, new_binder)


_U = TypeVar("_U")


class RayContextError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class RayContext(BaseContainer, SupportsKind1["RayContext", _T], Applicative1[_T]):
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
        if not issubclass(container.wrapped, RayFunctionNode):
            raise RayContextError(
                "A RayFunctionNode is expected for applying to a RayContext"
            )

        # Get the wrapped value of the container
        self_wrap = self.wrapped

        if issubclass(container.wrapped, RayFunctionNode):
            return self._apply_context(container)
        raise NotImplementedError

    def map(self, function: Callable[[_T], _U]) -> "RayContext[_U]":
        """
        Maps a function over a container.
        """
        # check if the value is a callable
        f_app = RayContext.from_value(function)
        return self.apply(f_app)
