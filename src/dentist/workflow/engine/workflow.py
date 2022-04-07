from . import executors
from .executors import AbstractExecutor, JobFailure
from .actions import AbstractAction
from pathlib import Path
import logging


__all__ = ["workflow", "MissingInputs"]

log = logging.getLogger(__name__)


def workflow(definition):
    def wrapper(
        *args,
        dry_run=False,
        print_commands=False,
        executor=None,
        threads=1,
        force=False,
        **kwargs,
    ):
        definition.__globals__["workflow"] = Workflow(
            name=definition.__name__,
            dry_run=dry_run,
            print_commands=print_commands,
            threads=threads,
            force=force,
        )
        definition(*args, **kwargs)
        definition.__globals__["workflow"].flush_jobs(final=True)

    return wrapper


class Workflow(object):
    def __init__(
        self,
        name,
        *,
        executor="LocalExecutor",
        dry_run=False,
        print_commands=False,
        threads=1,
        force=False,
    ):
        self.name = name
        self.executor = self.__make_executor(executor)
        self.dry_run = dry_run
        self.print_commands = print_commands
        self.threads = threads
        self.force = force
        self.job_queue = []
        self.jobs = dict()

    def __make_executor(self, executor):
        if isinstance(executor, AbstractExecutor):
            return executor
        elif isinstance(executor, str):
            executor_class = getattr(executors, executor)
            return executor_class()
        elif issubclass(executor, AbstractExecutor):
            return executor()

    def enqueue_job(self, *, name, inputs, outputs, action):
        params = self.__preprare_params(locals().copy())
        action = self.__prepare_action(action, params)
        return self.__enqueue_job(Job(action=action, **params))

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
            # mark jobs as done
            for job in self.job_queue:
                job.done()
            # reset job queue
            self.job_queue = []
        except JobFailure as job_failure:
            self._discard_files(job_failure.outputs())
            raise job_failure


class Job(AbstractAction):
    def __init__(self, *, name, inputs, outputs, action):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.action = action
        self._is_done = False

    @property
    def is_done(self):
        return self._is_done

    def done(self):
        self._is_done = True

    def to_command(self):
        return self.action.to_command()


class DuplicateJob(Exception):
    def __init__(self, existing, duplicate):
        super().__init__()
        self.existing = existing
        self.duplicate = duplicate

    def __str__(self):
        return f"duplicate job `{self.existing.name}`"


class MissingInputs(Exception):
    INDENT = "  "

    def __init__(self, files):
        super().__init__()
        self.files = files

    def __str__(self):
        fnames = f"\n{self.INDENT}".join(str(f) for f in self.files)

        return f"missing input file(s):\n{self.INDENT}{fnames}"
