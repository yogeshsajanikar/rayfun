# Copyright 2023 Yogesh Sajanikar
import ray

from rayfun.types import from_iterable, parallel_reduce


def test_list_reducer(ray_start):
    inputs = range(10)
    expected = sum(inputs)

    def reducer(a, b):
        return a + b

    def identity(a):
        return a

    ray_inputs = from_iterable(inputs)
    actual = parallel_reduce(ray_inputs, identity, reducer)

    assert ray.get(actual.wrapped.reduce().wrapped) == expected
