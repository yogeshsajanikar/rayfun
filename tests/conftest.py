# Copyright 2023 Yogesh Sajanikar

import ray
import pytest


@pytest.fixture(scope="session")
def ray_start():
    ray.init()
    yield
    ray.shutdown()
