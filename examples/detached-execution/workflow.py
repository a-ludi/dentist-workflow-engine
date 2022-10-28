from pathlib import Path

from dentist import ShellScript, safe, workflow


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
                ("sleep", "0.1"),
                ("echo", f"data-{i:05d}", safe(">"), outputs[0]),
            ),
        )
        generated_files.extend(job.outputs)

    workflow.execute_jobs()

    final_job = workflow.collect_job(
        name="concat_results",
        inputs=generated_files,
        outputs=[outdir / "combined.out"],
        action=lambda inputs, outputs: ShellScript(
            ("cat", *inputs, safe(">"), outputs[0])
        ),
    )

    workflow.execute_jobs()
    with final_job.outputs[0].open() as file:
        expected = "\n".join(f"data-{i:05d}" for i in range(count)) + "\n"
        assert file.read() == expected


def submit_jobs(jobs):
    from subprocess import Popen

    for job in jobs:
        Popen(job.action.to_command())

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
