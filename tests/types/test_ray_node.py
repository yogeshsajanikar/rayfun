# Copyright 2023 Yogesh Sajanikar
from typing import Callable

import pytest
import ray
from hypothesis import strategies as st
from returns.contrib.hypothesis.containers import strategy_from_container

from rayfun.types import (
    RayObjectNode,
    RayFunctionNode,
    RayFinalFunctionNode,
    RayContext,
    RayContextError,
)

# from returns.contrib.hypothesis.laws import check_all_laws

st.register_type_strategy(
    RayContext,
    strategy_from_container(RayContext),
)


def test_ray_object_node(ray_start):
    ref = ray.put(10)
    obj_ref = RayObjectNode[int](ref)
    assert obj_ref.execute() == obj_ref
    assert obj_ref.execute() != 10


@ray.remote
def add(a: int, b: int) -> int:
    return a + b


def test_ray_function_node(ray_start):
    remote_add = RayFunctionNode[Callable[[int, int], int]].from_remote_function(add)
    remote_1 = RayObjectNode[int](ray.put(1))
    remote_2 = RayObjectNode[int](ray.put(2))
    remote_add_1 = remote_add.apply_arg(remote_1)
    remote_add_2 = remote_add_1.apply_arg(remote_2)

    f_node = remote_add_2.execute()
    assert isinstance(f_node, RayFinalFunctionNode)
    answer_ref = f_node.execute()
    assert isinstance(answer_ref, RayObjectNode)
    assert ray.get(answer_ref._inner_value) == 3


def test_final_function_node_exception(ray_start):
    remote_add = RayFunctionNode[Callable[[int, int], int]].from_remote_function(add)
    remote_1 = RayObjectNode[int](ray.put(1))
    remote_add_1 = remote_add.apply_arg(remote_1)
    remote_add_2 = remote_add_1.execute()
    assert isinstance(remote_add_2, RayFunctionNode)


def test_final_function_node_apply_exception(ray_start):
    with pytest.raises(TypeError):
        remote_add = RayFunctionNode[Callable[[int, int], int]].from_remote_function(
            add
        )
        remote_1 = RayObjectNode[int](ray.put(1))
        remote_add_1 = remote_add.apply_arg(remote_1)
        remote_add_2 = remote_add_1.apply_arg(remote_1)
        remote_add_2.apply_arg(remote_1)


def identity_func(x):
    return x


def test_raycontext_functor_identity_law(ray_start):
    ray_value = RayContext.from_value(10).map(identity_func)
    id_value = identity_func(10)
    ray_store_value = ray_value.wrapped.reduce().execute()
    assert ray.get(ray_store_value.wrapped) == id_value


def test_raycontext_functor_composition_law(ray_start):
    def f(x):
        return x + 1

    def g(x):
        return x * 2

    ray_value = RayContext.from_value(10).map(f).map(g)
    ray_store_value = ray_value.wrapped.reduce().execute()
    assert ray.get(ray_store_value.wrapped) == g(f(10))


def test_raycontext_applicative_identity_law(ray_start):
    ray_value = RayContext.from_value(10)
    ray_identity = RayContext.from_value(identity_func)
    ray_store_value = ray_value.apply(ray_identity).wrapped.reduce().execute()
    assert ray.get(ray_store_value.wrapped) == identity_func(10)


def test_raycontext_applicative_homomorphism_law(ray_start):
    def f(x):
        return x + 1

    ray_value = RayContext.from_value(10)
    ray_f = RayContext.from_value(f)
    ray_store_value = ray_value.apply(ray_f).wrapped.reduce().execute()
    assert ray.get(ray_store_value.wrapped) == f(10)


def test_raycontext_applicative_associative_law(ray_start):
    def func(x):
        return x + 1

    raw_value = 10

    ray_value = RayContext.from_value(raw_value)
    ray_f = RayContext.from_value(func)

    raw_to_func = ray_value.apply(ray_f)

    def func2(f):
        return ray.get(f.remote(raw_value))

    ray_f2 = RayContext.from_value(func2)

    func_to_raw = ray_f.apply(ray_f2)

    ray_store_1 = raw_to_func.wrapped.reduce().execute()
    ray_store_2 = func_to_raw.wrapped.reduce().execute()
    assert ray.get(ray_store_1.wrapped) == ray.get(ray_store_2.wrapped)


def test_raycontext_wrong_application(ray_start):
    with pytest.raises(RayContextError):
        ray_value_1 = RayContext.from_value(10)
        ray_value_2 = RayContext.from_value(20)
        ray_value_1.apply(ray_value_2)


# TODO: Though ideal, the hypothesis classes cannot be serialized by Ray.
# check_all_laws(RayContext, settings_kwargs={"max_examples": 500})
