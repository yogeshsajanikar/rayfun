# Copyright 2023 Yogesh Sajanikar
import ray

from rayfun.types import RayContext, from_iterable, parallel_reduce


def test_list_reducer(ray_start):
    inputs = range(10)
    expected = sum(inputs) + 29

    def reducer(a, b):
        return a + b

    def identity(a):
        return a

    ray_inputs = from_iterable(inputs)
    ray_identity = RayContext.from_value(identity)
    ray_reducer = RayContext.from_value(reducer)
    ray_initial = RayContext.from_value(29)
    actual = parallel_reduce(ray_inputs, ray_initial, ray_identity, ray_reducer)

    assert ray.get(actual.wrapped.reduce().wrapped) == expected

    actual_initial = parallel_reduce(iter([]), ray_initial, ray_identity, ray_reducer)
    assert ray.get(actual_initial.run().wrapped) == 29

    single_input = [11]
    single_expected = sum(single_input) + 29

    single_ray_inputs = from_iterable(single_input)
    single_actual = parallel_reduce(
        single_ray_inputs, ray_initial, ray_identity, ray_reducer
    )
    assert ray.get(single_actual.wrapped.reduce().wrapped) == single_expected
