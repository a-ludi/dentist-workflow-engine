from dentist import *
from pathlib import Path


@workflow
def example_workflow(*, indir, outdir):
    def to_upper_case():
        return ShellScript(
            ShellCommand(["tr", "a-z", "A-Z"], stdin=inputs[0], stdout=outputs[0])
        )

    # make sure outdir exists
    outdir.mkdir(parents=True, exist_ok=True)

    workflow.enqueue_job(
        name="transform_foo",
        inputs=[indir / "foo.in"],
        outputs=[outdir / "foo.out"],
        action=to_upper_case,
    )
    workflow.enqueue_job(
        name="transform_bar",
        inputs=[indir / "bar.in"],
        outputs=[outdir / "bar.out"],
        action=to_upper_case,
    )

    workflow.flush_jobs()

    workflow.enqueue_job(
        name="combine_results",
        inputs=[
            *workflow.jobs["transform_foo"].outputs,
            *workflow.jobs["transform_bar"].outputs,
        ],
        outputs=[outdir / "result.out"],
        action=lambda: ShellScript(ShellCommand(["cat", *inputs], stdout=outputs[0])),
    )


def main():
    import logging
    import sys

    logging.basicConfig(level=logging.DEBUG)
    script_root = Path(sys.argv[0]).parent
    example_workflow(
        indir=script_root / "inputs",
        outdir=script_root / "results",
        dry_run=False,
        print_commands=True,
    )


if __name__ == "__main__":
    main()
