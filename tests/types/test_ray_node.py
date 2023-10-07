# Copyright 2023 Yogesh Sajanikar
from typing import Callable

import pytest
import ray
from hypothesis import strategies as st
from returns.contrib.hypothesis.containers import strategy_from_container
from returns.contrib.hypothesis.laws import check_all_laws

from rayfun.types import (
    RayObjectNode,
    RayFunctionNode,
    RayFinalFunctionNode,
    RayContext,
)

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
    with pytest.raises(TypeError):
        remote_add = RayFunctionNode[Callable[[int, int], int]].from_remote_function(
            add
        )
        remote_1 = RayObjectNode[int](ray.put(1))
        remote_add_1 = remote_add.apply_arg(remote_1)
        remote_add_1.execute()


def test_final_function_node_apply_exception(ray_start):
    with pytest.raises(TypeError):
        remote_add = RayFunctionNode[Callable[[int, int], int]].from_remote_function(
            add
        )
        remote_1 = RayObjectNode[int](ray.put(1))
        remote_add_1 = remote_add.apply_arg(remote_1)
        remote_add_2 = remote_add_1.apply_arg(remote_1)
        remote_add_2.apply_arg(remote_1)


check_all_laws(RayContext, settings_kwargs={"max_examples": 500})
