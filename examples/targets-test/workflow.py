from itertools import chain
from pathlib import Path

from dentist import ShellScript, cli_parser, safe, workflow


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

    workflow.collect_job(
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


def main():
    import logging
    import sys

    script_root = Path(sys.argv[0]).parent
    parser = cli_parser(log_level=True)
    parser.add_argument(
        "--count",
        metavar="<int>",
        type=int,
        default=100,
        help="generate <int> files in parallel",
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
    del params["targets"]
    logging.basicConfig(level=params.pop("log_level"))
    example_workflow(targets={"generate.1"}, **params)

    assert (params["outdir"] / "file_1").exists()
    for i in range(2, params["count"] + 1):
        assert not (params["outdir"] / f"file_{i}").exists()
    assert not (params["outdir"] / "combined.out").exists()


if __name__ == "__main__":
    main()
