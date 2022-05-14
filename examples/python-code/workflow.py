from pathlib import Path

from dentist import ShellCommand, ShellScript, Workflow, python_code


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
        self.collect_job(
            name="create_outdir",
            inputs=[],
            outputs=[self.outdir],
            exec_local=True,
            action=self.create_outdirs,
        )
        self.execute_jobs()

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
            exec_local=True,
            action=self.concat_files,
        )

    def check_output(self):
        self.execute_jobs()
        with self.jobs["combine_results"].outputs[0].open() as file:
            assert file.read() == "FOO-DATA\nBAR-DATA\n"

    @staticmethod
    @python_code
    def create_outdirs(outputs):
        for outdir in outputs:
            outdir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def to_upper_case(inputs, outputs):
        return ShellScript(
            ShellCommand(["tr", "a-z", "A-Z"], stdin=inputs[0], stdout=outputs[0])
        )

    @staticmethod
    @python_code
    def concat_files(inputs, outputs):
        with open(outputs[0], "w") as out:
            for input in inputs:
                with open(input) as infile:
                    out.write(infile.read())


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
