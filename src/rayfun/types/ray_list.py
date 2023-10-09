# Copyright 2023 Yogesh Sajanikar
from __future__ import annotations

from typing import Callable, List, Iterable, Tuple, TypeVar
from .ray_node import RayContext

_T = TypeVar("_T")
_U = TypeVar("_U")
_V = TypeVar("_V")


def from_iterable(iterable: Iterable[_T]) -> Iterable[RayContext[_T]]:
    """
    Returns an iterable from the given iterable.
    """
    for item in iterable:
        yield RayContext.from_value(item)


def _apply_combiner(
    first: RayContext[_T],
    second: RayContext[_U],
    combiner: RayContext[Callable[[_T, _U], _V]],
) -> RayContext[_V]:
    first_ap: RayContext[Callable[[_U], _V]] = first.apply(combiner)
    second_ap: RayContext[_V] = second.apply(first_ap)
    return second_ap


def _parallel_combine(
    input: List[RayContext[_T]], combiner: RayContext[Callable[[_T, _T], _T]]
) -> RayContext[_T]:
    if len(input) == 1:
        return input[0]

    shifted = input[1:]
    zipped = list(zip(input, shifted))
    pairs = zipped[::2]

    next_pairs = [_apply_combiner(first, second, combiner) for first, second in pairs]
    result = _parallel_combine(next_pairs, combiner)

    if len(input) % 2 == 1:
        return _apply_combiner(result, input[-1], combiner)
    else:
        return result


def parallel_combine(
    input: List[RayContext[_T]], combine: Callable[[_T, _T], _T]
) -> RayContext[_T]:
    """
    Combines the given list in parallel.
    """
    if len(input) < 0:
        raise ValueError("input must be non-empty")

    if len(input) == 1:
        return input[0]

    else:
        combiner = RayContext.from_value(combine)
        return _parallel_combine(input, combiner)


def parallel_reduce(
    iterable: Iterable[RayContext[_T]],
    function: Callable[[_T], _U],
    combine: Callable[[_U, _U], _U],
) -> RayContext[_U]:
    """
    Reduces the iterable in parallel.
    """
    map_list: List[RayContext[_U]] = [
        mapped for mapped in map(lambda x: x.map(function), iterable)
    ]
    return parallel_combine(map_list, combine)
