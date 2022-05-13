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

    workflow.collect_job(
        name="transform_foo",
        inputs=[indir / "foo.in"],
        outputs=[outdir / "foo.out"],
        action=to_upper_case,
    )
    workflow.collect_job(
        name="transform_bar",
        inputs=[indir / "bar.in"],
        outputs=[outdir / "bar.out"],
        action=to_upper_case,
    )

    workflow.execute_jobs()

    workflow.collect_job(
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
    indir = script_root / "inputs"
    outdir = script_root / "results"
    parser = cli_parser()
    parser.add_argument(
        "--indir",
        metavar="<dir>",
        type=Path,
        default=script_root / "inputs",
        help="directory with input files; defaults to `./inputs` relative to "
        "the workflow file",
    )
    parser.add_argument(
        "--outdir",
        metavar="<dir>",
        type=Path,
        default=script_root / "results",
        help="directory where output files are written; defaults to "
        "`./results` relative to the workflow file",
    )
    params = vars(parser.parse_args())
    example_workflow(**params)


if __name__ == "__main__":
    main()
