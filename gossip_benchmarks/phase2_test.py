import ray


ray.init(
    _system_config={
        "enable_recovery_succession": True,
    },
)


@ray.remote(max_retries=1)
def produce():
    return 21


@ray.remote(max_retries=1)
def consume_direct(value):
    # A top-level ObjectRef is automatically resolved by Ray.
    return value * 2


@ray.remote(max_retries=1)
def consume_nested(values):
    # An ObjectRef inside a list remains an ObjectRef.
    return ray.get(values[0]) + 1


produced = produce.remote()

direct_result = consume_direct.remote(produced)
nested_result = consume_nested.remote([produced])

assert ray.get(direct_result) == 42
assert ray.get(nested_result) == 22

print("Phase 2 enabled test passed.")

ray.shutdown()