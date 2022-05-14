from pathlib import Path

from dentist import ShellScript, Workflow, safe


class ExampleWorkflow(Workflow):
    def __init__(self, *args, indir, outdir, **kwargs):
        super().__init__(*args, **kwargs)
        self.indir = indir
        self.outdir = outdir

    def run(self):
        self.create_outdir()
        self.transform_phase()
        self.combine_phase()
        self.check_output()

    def create_outdir(self):
        self.outdir.mkdir(parents=True, exist_ok=True)

    def transform_phase(self):
        self.collect_job(
            name="transform_foo",
            inputs=[self.indir / "foo.in"],
            outputs=[self.outdir / "foo.out"],
            action=self.to_upper_case,
        )
        self.collect_job(
            name="transform_bar",
            inputs=[self.indir / "bar.in"],
            outputs=[self.outdir / "bar.out"],
            action=self.to_upper_case,
        )

        self.execute_jobs()

    def combine_phase(self):
        self.collect_job(
            name="combine_results",
            inputs=[
                *self.jobs["transform_foo"].outputs,
                *self.jobs["transform_bar"].outputs,
            ],
            outputs=[self.outdir / "result.out"],
            action=lambda inputs, outputs: ShellScript(
                ("cat", *inputs, safe(">"), outputs[0])
            ),
        )

    def check_output(self):
        self.execute_jobs()
        with self.jobs["combine_results"].outputs[0].open() as file:
            assert file.read() == "FOO-DATA\nBAR-DATA\n"

    @staticmethod
    def to_upper_case(inputs, outputs):
        return ShellScript(
            ("tr", "a-z", "A-Z", safe("<"), inputs[0], safe(">"), outputs[0])
        )


def main():
    import logging
    import sys

    logging.basicConfig(level=logging.DEBUG)
    script_root = Path(sys.argv[0]).parent
    workflow = ExampleWorkflow(
        indir=script_root / "inputs",
        outdir=script_root / "results",
        dry_run=False,
        print_commands=True,
    )
    workflow()


if __name__ == "__main__":
    main()
