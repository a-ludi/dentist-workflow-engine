from . import executors
from .executors import AbstractExecutor, JobFailed
from .actions import AbstractAction
from enum import Enum
from hashlib import md5
from pathlib import Path
from shutil import rmtree
import logging


__all__ = [
    "DuplicateJob",
    "FaultyFilesException",
    "IncompleteOutputs",
    "MissingInputs",
    "workflow",
]

log = logging.getLogger(__name__)


def workflow(definition):
    def make_executor(executor, *, submit_jobs, check_delay):
        if executor is None:
            if submit_jobs is None:
                return executors.LocalExecutor()
            else:
                return executors.DetachedExecutor(
                    submit_jobs=submit_jobs,
                    check_delay=check_delay,
                )

        if isinstance(executor, AbstractExecutor):
            return executor
        elif isinstance(executor, str):
            executor_class = getattr(executors, executor)
            return executor_class()
        elif issubclass(executor, AbstractExecutor):
            return executor()

    def wrapper(
        *args,
        workflow_root=None,
        dry_run=False,
        print_commands=False,
        executor=None,
        submit_jobs=None,
        check_delay=15,
        threads=1,
        force=False,
        workflow_dir=".workflow",
        **kwargs,
    ):
        if workflow_root is None:
            from sys import argv

            workflow_root = Path(argv[0]).parent

        definition.__globals__["workflow"] = Workflow(
            name=definition.__name__,
            workflow_root=workflow_root,
            executor=make_executor(
                executor,
                submit_jobs=submit_jobs,
                check_delay=check_delay,
            ),
            dry_run=dry_run,
            print_commands=print_commands,
            threads=threads,
            force=force,
            workflow_dir=workflow_dir,
        )
        definition(*args, **kwargs)
        definition.__globals__["workflow"].flush_jobs(final=True)

    return wrapper


class Workflow(object):
    def __init__(
        self,
        name,
        *,
        executor,
        workflow_root,
        dry_run=False,
        print_commands=False,
        threads=1,
        force=False,
        workflow_dir=".workflow",
    ):
        self.name = name
        self.executor = executor
        self.dry_run = dry_run
        self.print_commands = print_commands
        self.threads = threads
        self.force = force
        self.job_queue = []
        self.jobs = dict()
        self.workflow_root = Path(workflow_root)
        self.workflow_dir = self.workflow_root / workflow_dir
        self.status_tracking = self.executor.requires_status_tracking

        if self.status_tracking:
            self.status_tracking_dir = self.workflow_dir / "jobs"
            if self.status_tracking_dir.exists():
                try:
                    rmtree(self.status_tracking_dir)
                except Exception as reason:
                    raise Exception(
                        f"could not delete status tracking directory: {reason}\n"
                        "\n"
                        f"Please delete it manually: {self.status_tracking_dir}"
                    )
            self.status_tracking_dir.mkdir(parents=True)

    def enqueue_job(self, *, name, inputs, outputs, action):
        params = self.__preprare_params(locals().copy())
        action = self.__prepare_action(action, params)
        job = self.__enqueue_job(Job(action=action, **params))

        if self.status_tracking:
            job.enable_tracking(self.status_tracking_dir / job.hash())

        return job

    def __preprare_params(self, params):
        del params["self"]
        del params["action"]

        for file_list in ("inputs", "outputs"):
            params[file_list] = self.__prepare_file_list(params[file_list])

        return params

    def __prepare_file_list(self, file_list):
        return [Path(file) for file in file_list]

    def __prepare_action(self, action, params):
        if callable(action):
            action.__globals__.update(params)
            action = action()
        else:
            action = action

        assert isinstance(action, AbstractAction)

        return action

    def __enqueue_job(self, job):
        self._check_inputs(job.inputs)
        if self.force or not self._is_up_to_date(job.inputs, job.outputs):
            force_suffix = (
                " (forced)" if self._is_up_to_date(job.inputs, job.outputs) else ""
            )
            log.debug(f"queued job {job.name}{force_suffix}")
            self.job_queue.append(job)
        else:
            log.debug(f"skipping job {job.name}: all outputs are up-to-date")

        if job.name not in self.jobs:
            self.jobs[job.name] = job

            return job
        else:
            raise DuplicateJob(self.jobs[job.name], job)

    def _check_inputs(self, inputs):
        missing_inputs = [input for input in inputs if not input.exists()]
        if len(missing_inputs) > 0:
            raise MissingInputs(missing_inputs)

    def _check_outputs(self, inputs, outputs):
        input_mtime = max(
            float("-inf"),
            float("-inf"),
            *(input.stat().st_mtime for input in inputs),
        )

        return [
            output
            for output in outputs
            if not output.exists() or output.stat().st_mtime < input_mtime
        ]

    def _is_up_to_date(self, inputs, outputs):
        input_mtime = max(
            float("-inf"),
            float("-inf"),
            *(input.stat().st_mtime for input in inputs),
        )
        output_mtime = min(
            float("inf"),
            float("inf"),
            *(
                output.stat().st_mtime if output.exists() else float("-inf")
                for output in outputs
            ),
        )

        if input_mtime == float("-inf"):
            # no input is up-to-date iff output exists
            return output_mtime > float("-inf")
        else:
            return input_mtime <= output_mtime

    def _discard_files(self, files):
        log.debug(f"discarding files: {', '.join(str(f) for f in files)}")
        for file in files:
            file.unlink(missing_ok=True)

    def flush_jobs(self, *, final=False):
        suffix = " (dry run)" if self.dry_run else ""

        if len(self.job_queue) > 0:
            self.__flush_jobs()
            if final:
                log.info("all jobs done" + suffix)
            else:
                log.debug("flushed jobs" + suffix)
        else:
            if final:
                log.info("nothing to be done" + suffix)
            else:
                log.debug("no jobs to be flushed" + suffix)

    def __flush_jobs(self):
        try:
            self.executor(
                self.job_queue,
                dry_run=self.dry_run,
                print_commands=self.print_commands,
                threads=self.threads,
            )

            incomplete_outputs = []
            for job in self.job_queue:
                assert job.state.is_done
                incomplete_outputs.extend(self._check_outputs(job.inputs, job.outputs))
            if len(incomplete_outputs) > 0:
                raise IncompleteOutputs(incomplete_outputs)

            # reset job queue
            self.job_queue = []
        except JobFailed as job_failure:
            self._discard_files(job_failure.job.outputs)
            raise job_failure


class JobState(Enum):
    DONE = 0
    WAITING = 1
    FAILED = 2

    @property
    def is_waiting(self):
        return self == JobState.WAITING

    @property
    def is_finished(self):
        return not self.is_waiting

    @property
    def is_done(self):
        return self == JobState.DONE

    @property
    def is_failed(self):
        return self == JobState.FAILED


class Job(AbstractAction):
    def __init__(self, *, name, inputs, outputs, action):
        self.name = name
        if not self.name.isidentifier():
            raise Exception("Job names must be valid Python identifiers.")
        self.inputs = inputs
        self.outputs = outputs
        self.action = action
        self.state = JobState.WAITING
        self.exit_code = -1
        self.id = None

    def done(self):
        assert not self.state.is_finished
        self.state = JobState.DONE
        self.exit_code = 0
        self.action.clean_up_tracking_status_file()

    def failed(self, exit_code):
        assert not self.state.is_finished
        self.state = JobState.FAILED
        self.exit_code = exit_code
        self.action.clean_up_tracking_status_file()

    def enable_tracking(self, status_path):
        super().enable_tracking(status_path)
        self.action.enable_tracking(status_path)

    def to_command(self):
        return self.action.to_command()

    def describe(self):
        if self.id is None:
            return f"`{self.name}`"
        else:
            return f"`{self.name}` (id={self.id})"

    def hash(self):
        return md5(bytes(self.name, "utf-8")).hexdigest()


class DuplicateJob(Exception):
    def __init__(self, existing, duplicate):
        super().__init__()
        self.existing = existing
        self.duplicate = duplicate

    def __str__(self):
        return f"duplicate job `{self.existing.name}`"


class FaultyFilesException(Exception):
    INDENT = "  "
    DESCRIPTION = "Faulty files"

    def __init__(self, files):
        super().__init__()
        self.files = files

    def __str__(self):
        fnames = f"\n{self.INDENT}".join(str(f) for f in self.files)

        return f"{description}:\n{self.INDENT}{fnames}"


class MissingInputs(FaultyFilesException):
    DESCRIPTION = "missing input file(s)"


class IncompleteOutputs(FaultyFilesException):
    DESCRIPTION = "missing or out-dated output file(s)"
