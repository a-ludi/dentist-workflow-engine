from dentist import *
from pathlib import Path


@workflow
def example_workflow(*, count, outdir):

    # make sure outdir exists
    outdir.mkdir(parents=True, exist_ok=True)

    generated_files = list()
    for i in range(count):
        job = workflow.enqueue_job(
            name=f"generate_{i}",
            inputs=[],
            outputs=[outdir / f"file_{i}"],
            action=lambda: ShellScript(
                ShellCommand(["sleep", "0.1"]),
                ShellCommand(["echo", f"data-{i:05d}"], stdout=outputs[0]),
            ),
        )
        generated_files.extend(job.outputs)

    workflow.flush_jobs()

    workflow.enqueue_job(
        name="concat_results",
        inputs=generated_files,
        outputs=[outdir / "combined.out"],
        action=lambda: ShellScript(ShellCommand(["cat", *inputs], stdout=outputs[0])),
    )


def main():
    import logging
    import sys

    logging.basicConfig(level=logging.DEBUG)
    script_root = Path(sys.argv[0]).parent
    example_workflow(
        count=100,
        threads=10,
        outdir=script_root / "results",
        dry_run=False,
        print_commands=True,
    )


if __name__ == "__main__":
    main()
