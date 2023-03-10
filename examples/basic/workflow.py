from pathlib import Path

from dentist import ShellScript, cli_parser, safe, workflow


@workflow
def example_workflow(workflow, *, indir, outdir):
    def to_upper_case(inputs, outputs):
        return ShellScript(
            ("tr", "a-z", "A-Z", safe("<"), inputs[0], safe(">"), outputs[0])
        )

    # make sure outdir exists
    outdir.mkdir(parents=True, exist_ok=True)
    logdir = outdir / "logs"
    logdir.mkdir(parents=True, exist_ok=True)

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

    final_job = workflow.collect_job(
        name="combine_results",
        inputs=[
            *workflow.jobs["transform_foo"].outputs,
            *workflow.jobs["transform_bar"].outputs,
        ],
        outputs=[outdir / "result.out"],
        log=logdir / "combine.log",
        action=lambda inputs, outputs: ShellScript(
            ("echo", "-n", "combining inputs..."),
            ("cat", *inputs, safe(">"), outputs[0]),
            ("echo", "done"),
        ),
    )

    workflow.execute_jobs()
    with final_job.outputs[0].open() as file:
        assert file.read() == "FOO-DATA\nBAR-DATA\n"
    with final_job.log.open() as file:
        assert file.read() == "combining inputs...done\n"


def main():
    import logging
    import sys

    script_root = Path(sys.argv[0]).parent
    parser = cli_parser(log_level=True)
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
    logging.basicConfig(level=params.pop("log_level"))
    example_workflow(**params)


if __name__ == "__main__":
    main()
