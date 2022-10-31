from pathlib import Path

from dentist import ShellScript, Workflow, safe


class ExampleWorkflow(Workflow):
    def __init__(self, *args, outdir, **kwargs):
        super().__init__(*args, **kwargs)
        self.outdir = outdir
        self.config_attrs.extend("outdir".split())

    def run(self):
        self.create_outdir()
        self.create_inputs()
        with self.grouped_jobs("transform_and_combine", temp_intermediates=True):
            self.transform_phase()
            self.combine_phase()
        self.finalize_output()
        self.check_output()

    def create_outdir(self):
        self.outdir.mkdir(parents=True, exist_ok=True)

    def create_inputs(self):
        self.collect_job(
            name="create_foo",
            inputs=[],
            outputs=[self.outdir / "foo.in"],
            action=self.create_file,
        )
        self.collect_job(
            name="create_bar",
            inputs=[],
            outputs=[self.outdir / "bar.in"],
            action=self.create_file,
        )

        self.execute_jobs()

    def transform_phase(self):
        self.collect_job(
            name="transform_foo",
            inputs=[self.outdir / "foo.in"],
            outputs=[self.outdir / "foo.out"],
            action=self.to_upper_case,
        )
        self.collect_job(
            name="transform_bar",
            inputs=[self.outdir / "bar.in"],
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
        self.execute_jobs()

    def finalize_output(self):
        self.collect_job(
            name="finalize_output",
            inputs=self.jobs["combine_results"].outputs,
            outputs=[self.outdir / "final-result.out"],
            action=lambda inputs, outputs: ShellScript(
                ("echo", "final-output", safe("\\")),
                (safe("|"), "cat", "-", *inputs, safe(">"), outputs[0]),
            ),
        )
        self.execute_jobs()

    def check_output(self):
        self.execute_jobs()
        lines = [
            "final-output",
            *(
                str(path).upper()
                for path in (
                    self.jobs["create_foo"].outputs[0],
                    self.jobs["create_bar"].outputs[0],
                )
            ),
        ]
        expected = "\n".join(lines) + "\n"
        with self.jobs["finalize_output"].outputs[0].open() as file:
            assert file.read() == expected

    @staticmethod
    def create_file(outputs):
        return ShellScript(("echo", outputs[0], safe(">"), outputs[0]))

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
        outdir=script_root / "results",
        dry_run=False,
        # keep_temp=True,
        # delete_temp=True,
        print_commands=True,
    )
    workflow()


if __name__ == "__main__":
    main()
