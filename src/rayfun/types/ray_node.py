# Copyright 2023 Yogesh Sajanikar
from abc import abstractmethod
from typing import TypeVar, Any

from ray import ObjectRef
from ray.remote_function import RemoteFunction
from returns.primitives.container import BaseContainer
from returns.primitives.hkt import SupportsKind1

_T = TypeVar("_T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_RayNodeType = TypeVar("_RayNodeType", bound="RayNode")


class RayNode(BaseContainer, SupportsKind1["RayNode", _T]):
    """
    The base class for all RayNode types.
    """

    def __init__(self, value: Any):
        super().__init__(value)

    @abstractmethod
    def execute(self) -> _T:
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

    def execute(self) -> _T:
        """
        Execute the node.
        """
        return self._inner_value


class RayFunctionNode(RayNode[_T]):
    """
    The base class for all RayFunctionNode types.
    """

    def __init__(self, value: RemoteFunction):
        super().__init__(value)
        self._inner_value = value

    def apply(self, arg: _T1) -> _RayNodeType:
        """
        Apply an argument to the function.
        """
        return self.__class__(self._inner_value.remote(arg))

    def execute(self) -> _T:
        """
        Execute the node.
        """
        return self._inner_value.execute()
