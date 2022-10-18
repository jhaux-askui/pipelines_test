from clearml import PipelineController


def step(size: int):
    print(f"starting step with size {size} ({type(size)}!")
    import numpy as np

    print("running step again...")

    return np.random.random(size=int(size))


def report(results):
    print(results)
    return results


pipe = PipelineController(
    name="ingest_v2",
    project="test_pipelines/johannes second pipline",
    version="0.1",
)

pipe.add_parameter(
    name="size",
    description="Size of the numpy array created in `step`",
    default=5,
    param_type="int",
)

pipe.set_default_execution_queue("single-gpu-2080ti")

pipe.add_function_step(
    name="step",
    function=step,
    function_kwargs={"size": "${pipeline.size}"},
    function_return=["le_array"],
    cache_executed_step=True,
    execution_queue="single-gpu-2080ti",
)

pipe.add_function_step(
    name="report result",
    function=report,
    function_kwargs={"results": "${step.le_array}"},
    function_return=["results"],
    cache_executed_step=False,
    execution_queue="single-gpu-2080ti",
    packages="requirements.txt",
)


if __name__ == "__main__":
    # pipe.start_locally(run_pipeline_steps_locally=True)
    pipe.start(queue="single-gpu-2080ti")
