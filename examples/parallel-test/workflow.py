from itertools import chain
from pathlib import Path

from dentist import ShellScript, safe, workflow


@workflow
def example_workflow(workflow, *, count, outdir):

    # make sure outdir exists
    outdir.mkdir(parents=True, exist_ok=True)

    for i in range(count):
        workflow.collect_job(
            name="generate",
            index=i,
            inputs=[],
            outputs=[outdir / f"file_{i}"],
            action=lambda outputs: ShellScript(
                ("sleep", "0.1"),
                ("echo", f"data-{i:05d}", safe(">"), outputs[0]),
            ),
        )
    workflow.execute_jobs()

    final_job = workflow.collect_job(
        name="concat_results",
        inputs=list(
            chain.from_iterable(
                job.outputs for job in workflow.jobs["generate"].values()
            )
        ),
        outputs=[outdir / "combined.out"],
        action=lambda inputs, outputs: ShellScript(
            ("cat", *inputs, safe(">"), outputs[0])
        ),
    )

    workflow.execute_jobs()
    with final_job.outputs[0].open() as file:
        expected = "\n".join(f"data-{i:05d}" for i in range(count)) + "\n"
        assert file.read() == expected


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
        resources="resources.json",
    )


if __name__ == "__main__":
    main()
