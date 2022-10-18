from clearml import PipelineDecorator


@PipelineDecorator.component(cache=True)
def step(size: int):
    print("starting step!")
    import numpy as np

    print("running step...")

    return np.random.random(size=size)


@PipelineDecorator.pipeline(
    name="ingest",
    project="test_pipelines/johannes first pipline",
    version="0.1",
    pipeline_execution_queue="single-gpu-2080ti",
    default_queue="single-gpu-3090",
    return_value="array",
)
def pipeline_logic(do_stuff: bool):
    if do_stuff:
        return step(size=5)


if __name__ == "__main__":
    # run the pipeline on the current machine, for local debugging
    # for scale-out, comment-out the following line and spin clearml agents
    # PipelineDecorator.run_locally()

    pipeline_logic(do_stuff=True)
