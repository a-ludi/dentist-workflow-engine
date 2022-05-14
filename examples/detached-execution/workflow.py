from pathlib import Path

from dentist import ShellCommand, ShellScript, workflow


@workflow
def example_workflow(workflow, *, count, outdir):

    # make sure outdir exists
    outdir.mkdir(parents=True, exist_ok=True)

    generated_files = list()
    for i in range(count):
        job = workflow.collect_job(
            name=f"generate_{i}",
            inputs=[],
            outputs=[outdir / f"file_{i}"],
            action=lambda inputs, outputs: ShellScript(
                ShellCommand(["sleep", "0.1"]),
                ShellCommand(["echo", f"data-{i:05d}"], stdout=outputs[0]),
            ),
        )
        generated_files.extend(job.outputs)

    workflow.execute_jobs()

    workflow.collect_job(
        name="concat_results",
        inputs=generated_files,
        outputs=[outdir / "combined.out"],
        action=lambda inputs, outputs: ShellScript(
            ShellCommand(["cat", *inputs], stdout=outputs[0])
        ),
    )


def submit_jobs(jobs):
    from subprocess import Popen

    for job in jobs:
        Popen(job.to_command())

    return list(range(len(jobs)))


def main():
    import logging
    import sys

    logging.basicConfig(level=logging.DEBUG)
    script_root = Path(sys.argv[0]).parent
    example_workflow(
        count=100,
        outdir=script_root / "results",
        dry_run=False,
        print_commands=True,
        submit_jobs=submit_jobs,
        check_delay=1,
    )


if __name__ == "__main__":
    main()
