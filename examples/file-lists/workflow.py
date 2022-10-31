from pathlib import Path

from dentist import FileList, ShellScript, Workflow, python_code, safe


class ExampleWorkflow(Workflow):
    def __init__(self, *args, indir, outdir, **kwargs):
        super().__init__(*args, **kwargs)
        self.indir = indir
        self.outdir = outdir
        self.logdir = outdir / "logs"
        self.config_attrs.extend("indir outdir logdir".split())

    def run(self):
        self.create_outdirs()
        self.transform_phase()
        self.combine_phase()
        self.check_output()

    def create_outdirs(self):
        self.collect_job(
            name="create_outdirs",
            inputs=[],
            outputs=[self.outdir, self.logdir],
            exec_local=True,
            action=self.act_create_outdirs,
        )
        self.execute_jobs()

    def transform_phase(self):
        self.collect_job(
            name="transform_foo",
            inputs=self.indir / "foo.in",
            outputs=self.outdir / "foo.out",
            action=self.to_upper_case,
        )
        self.collect_job(
            name="transform_bar",
            inputs=self.indir / "bar.in",
            outputs=self.outdir / "bar.out",
            action=self.to_upper_case,
        )

        self.execute_jobs()

    def combine_phase(self):
        @self.collect_job(
            inputs=FileList(
                foo=self.jobs["transform_foo"].output,
                bar=self.jobs["transform_bar"].output,
            ),
            outputs=self.outdir / "result.out",
            log=self.logdir / "combine.log",
            exec_local=True,
        )
        @python_code
        def combine_results(inputs, outputs, log):
            with open(log, "w") as log_fp:
                log_fp.write("combining inputs...")
                with open(outputs[0], "w") as out:
                    for key in ("foo", "bar"):
                        with open(inputs[key]) as infile:
                            out.write(infile.read())
                log_fp.write("done\n")

    def check_output(self):
        self.execute_jobs()
        with self.jobs["combine_results"].outputs[0].open() as file:
            assert file.read() == "FOO-DATA\nBAR-DATA\n"
        with self.jobs["combine_results"].log.open() as file:
            assert file.read() == "combining inputs...done\n"

    @staticmethod
    @python_code
    def act_create_outdirs(outputs):
        for outdir in outputs:
            outdir.mkdir(parents=True, exist_ok=True)

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
