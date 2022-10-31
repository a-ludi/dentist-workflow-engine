import logging
from contextlib import contextmanager
from enum import Enum
from hashlib import md5
from importlib import import_module
from itertools import chain
from pathlib import Path
from sys import argv

from . import executors
from .actions import AbstractAction
from .container import FileList, MultiIndex
from .executors import AbstractExecutor, JobFailed, LocalExecutor
from .resources import RootResources
from .util import discard_files, inject, throws
from .workdir import Workdir

__all__ = [
    "DuplicateJob",
    "FaultyFilesException",
    "IncompleteOutputs",
    "MissingInputs",
    "workflow",
    "Workflow",
]

_package_name = __name__
log = logging.getLogger(__name__)


def workflow(definition):
    def wrapper(
        *args,
        workflow_root=None,
        dry_run=False,
        print_commands=False,
        touch=False,
        delete_outputs=False,
        executor=None,
        local_executor=None,
        submit_jobs=None,
        check_delay=15,
        threads=1,
        force=False,
        keep_temp=False,
        force_delete_temp=False,
        workflow_dir=".workflow",
        resources=None,
        debug_flags=set(),
        **kwargs,
    ):
        _workflow = Workflow(
            definition=definition,
            workflow_root=workflow_root,
            dry_run=dry_run,
            print_commands=print_commands,
            touch=touch,
            delete_outputs=delete_outputs,
            executor=executor,
            local_executor=local_executor,
            submit_jobs=submit_jobs,
            check_delay=check_delay,
            threads=threads,
            force=force,
            keep_temp=keep_temp,
            force_delete_temp=force_delete_temp,
            workflow_dir=workflow_dir,
            resources=resources,
            debug_flags=debug_flags,
        )
        _workflow(*args, **kwargs)

    return wrapper


class Workflow(object):
    def __init__(
        self,
        *,
        # --- functional interface ---
        definition=None,
        # --- directories ---
        workflow_root=None,
        workflow_dir=".workflow",
        # --- operation control flags ---
        dry_run=False,
        force=False,
        keep_temp=False,
        force_delete_temp=False,
        print_commands=False,
        touch=False,
        delete_outputs=False,
        # --- execution configuration ---
        threads=1,
        resources=None,
        executor=None,
        local_executor=LocalExecutor,
        submit_jobs=None,
        check_delay=15,
        # --- debug flags ---
        debug_flags=set(),
    ):
        log.info("Welcome to the DENTIST workflow engine!")

        # --- inheritance/functional interface ---
        self.definition = definition
        if definition is not None:
            self.name = definition.__name__
        elif type(self) != Workflow:
            self.name = type(self).__name__
        else:
            raise ValueError(
                "either definition must be given or class must be derived from Workflow"
            )

        # --- debug flags ---
        self.debug_flags = debug_flags

        # --- directories ---
        if workflow_root is None:
            workflow_root = Path(argv[0]).parent
        else:
            workflow_root = Path(workflow_root)
        log.debug(f"workflow_root={workflow_root}")
        self.workdir = Workdir(workflow_root / workflow_dir)
        log.debug(f"workdir={self.workdir}")

        # --- operation control flags ---
        self.dry_run = dry_run or delete_outputs
        self.print_commands = print_commands
        self.touch = touch
        self.delete_outputs = delete_outputs
        if self.touch and self.delete_outputs:
            raise ValueError("must not provide both `touch` and `delete_outputs`")
        self.force = force or delete_outputs
        self.keep_temp = keep_temp or delete_outputs
        self.force_delete_temp = force_delete_temp and not delete_outputs
        if self.keep_temp and self.force_delete_temp:
            raise ValueError(
                "must not provide both `force_delete_temp` and `keep_temp`"
            )

        # --- execution configuration ---

        def force_executor(cause_flag, forced_executor):
            nonlocal submit_jobs, executor, local_executor

            if submit_jobs is not None:
                log.warning(f"ignoring `submit_jobs` in due to `{cause_flag}`")
            submit_jobs = None

            if executor is not None:
                log.warning(f"ignoring `executor` in due to `{cause_flag}`")
            executor = forced_executor

            if local_executor != forced_executor:
                log.warning(f"ignoring `local_executor` in due to `{cause_flag}`")
            local_executor = executor

        if self.touch:
            force_executor("touch", executors.TouchExecutor)
        if self.delete_outputs:
            force_executor("delete_outputs", local_executor)

        self.threads = threads
        if resources is None:
            self.resources = RootResources()
        else:
            self.resources = RootResources.read(workflow_root / resources)
        job_scripts_dir = self.workdir.acquire_dir("job-scripts", force_empty=True)
        self.executor = self.make_executor(
            executor,
            submit_jobs=submit_jobs,
            check_delay=check_delay,
            job_scripts_dir=job_scripts_dir,
            debug_flags=self.debug_flags,
        )
        self.status_tracking = self.executor.requires_status_tracking
        self.local_executor = self.make_executor(
            local_executor,
            submit_jobs=None,
            check_delay=check_delay,
            job_scripts_dir=job_scripts_dir,
            debug_flags=self.debug_flags,
        )
        self.local_status_tracking = self.local_executor.requires_status_tracking
        if self.status_tracking or self.local_status_tracking:
            self.status_tracking_dir = self.workdir.acquire_dir(
                "status", force_empty=True
            )

        # --- job handling ---
        self.job_queue = []
        self.jobs = dict()
        self._collect_group = False
        self._group_job_batches = []
        self._group_job_name = None

        # --- config reporting ---
        self.config_attrs = """
            name
            debug_flags
            workdir
            dry_run
            print_commands
            touch
            delete_outputs
            force
            keep_temp
            force_delete_temp
            threads
            resources
            executor
            status_tracking
            local_executor
            local_status_tracking
        """.split()

    @staticmethod
    def make_executor(
        executor, *, submit_jobs, check_delay, job_scripts_dir, debug_flags
    ):
        if isinstance(submit_jobs, str):
            submitter = import_module(f"..interfaces.{submit_jobs}", _package_name)
            submit_jobs = submitter.submit_jobs

        optargs = {
            "workdir": job_scripts_dir,
            "debug_flags": debug_flags,
        }

        if executor is None:
            if submit_jobs is None:
                return executors.LocalExecutor(optargs=optargs)
            else:
                return executors.DetachedExecutor(
                    submit_jobs=submit_jobs,
                    check_delay=check_delay,
                    optargs=optargs,
                )

        if isinstance(executor, AbstractExecutor):
            return executor
        elif isinstance(executor, str):
            executor_class = getattr(executors, executor)
            return executor_class(optargs=optargs)
        elif issubclass(executor, AbstractExecutor):
            return executor(optargs=optargs)

    def __call__(self, *args, **kwargs):
        self.pre_run(*args, **kwargs)
        try:
            self.run(*args, **kwargs)
            self.execute_jobs(final=True)
        except AssertionError as error:
            raise error
        except Exception as reason:
            if self.delete_outputs:
                last_job = list(self.jobs.values())[-1]
                if isinstance(last_job, dict):
                    last_job = list(last_job.values())[-1]
                log.warning(f"workflow stopped at job {last_job.describe()}")
                log.debug(f"reason for stopping: {reason}")
            else:
                raise reason
        finally:
            if self.delete_outputs:
                self.__delete_collected_outputs()
        self.post_run()

    def pre_run(self, *args, **kwargs):
        log.info(f"Executing workflow `{self.name}`")
        log.info(f"effective config: {str(self.get_config())}")

    def run(self, *args, **kwargs):
        if self.definition is None:
            if type(self) == Workflow:
                raise ValueError("`definition` is missing")
            else:
                raise NotImplementedError("`self.run()` not implemented")

        self.definition(self, *args, **kwargs)

    def post_run(self):
        log.info(f"Workflow `{self.name}` finished.")

    def get_config(self, attr=None):
        if attr is None:
            config = dict()
            for attr in self.config_attrs:
                config[attr] = self.get_config(attr)
            return config

        value = getattr(self, attr)

        if isinstance(value, set):
            value = list(value)
        elif isinstance(value, AbstractExecutor):
            value = type(value).__name__

        return value

    def collect_job(
        self,
        *,
        name=None,
        index=None,
        exec_local=False,
        inputs,
        outputs,
        action=None,
        log=None,
        resources=None,
        pre_conditions=[],
        post_conditions=[],
    ):
        if name is None and action is None:
            # act as function decorator
            def collector(action):
                return self.collect_job(
                    action=action,
                    name=action.__name__,
                    index=index,
                    exec_local=exec_local,
                    inputs=inputs,
                    outputs=outputs,
                    log=log,
                    resources=resources,
                    pre_conditions=pre_conditions,
                    post_conditions=post_conditions,
                )

            return collector
        elif name is None:
            raise ValueError("job is missing `name`")
        elif action is None:
            raise ValueError("job is missing `action`")

        params = self.__preprare_params(locals().copy())
        action = self.__prepare_action(action, params)
        job = self.__collect_job(Job(action=action, **params))

        if (not exec_local and self.status_tracking) or (
            exec_local and self.local_status_tracking
        ):
            job.enable_tracking(self.status_tracking_dir.acquire_file(job.hash))

        return job

    def __preprare_params(self, params):
        del params["self"]
        del params["action"]

        if params["log"] is not None:
            params["log"] = Path(params["log"])
        for file_list in ("inputs", "outputs"):
            params[file_list] = self.__prepare_file_list(params[file_list])

        if "resources" in params:
            if isinstance(params["resources"], dict):
                # user-supplied resource overrides
                base_res = self.resources[params["name"]]
                params["resources"] = base_res | params["resources"]
            else:
                # user-supplied resource identifier
                params["resources"] = self.resources[params["resources"]]
        else:
            # use job name as resource identifier
            params["resources"] = self.resources[params["name"]]

        # add basic file conditions
        params["pre_conditions"].insert(0, self.check_inputs)
        params["post_conditions"].append(self.check_up_to_date)
        # params["post_conditions"].append(self.check_outputs)

        return params

    def __prepare_file_list(self, file_list):
        return FileList.from_any(file_list)

    def __prepare_action(self, action, params):
        if callable(action) and not isinstance(action, AbstractAction):
            action = inject(action, **params)()
        else:
            action = action

        assert isinstance(action, AbstractAction)

        return action

    def __collect_job(self, job):
        if self._collect_group:
            self.job_queue.append(job)
        else:
            job.check_pre_conditions()
            post_check = job.check_post_conditions(return_bool=True)

            if self.force or not post_check:
                force_suffix = " (forced)" if post_check else ""
                log.debug(f"queued job {job.describe()}{force_suffix}")
                self.job_queue.append(job)
            else:
                log.debug(f"skipping job {job.describe()}: all outputs are up-to-date")

        if job.index is None:
            jobs_db = self.jobs
            job_id = job.name
        else:
            if job.name not in self.jobs:
                self.jobs[job.name] = dict()
            jobs_db = self.jobs[job.name]
            job_id = job.index

        if job_id not in jobs_db:
            jobs_db[job_id] = job
        else:
            raise DuplicateJob(jobs_db[job_id], job)

        return job

    @staticmethod
    def check_inputs(job, inputs):
        missing_inputs = [input for input in inputs if not input.exists()]
        if len(missing_inputs) > 0:
            raise MissingInputs([(job, missing_inputs)])

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

    @staticmethod
    def check_up_to_date(inputs, outputs):
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

        if output_mtime == float("-inf"):
            raise Exception("missing outputs")
        elif input_mtime > output_mtime:
            raise Exception("inputs are newer than outputs")

    def __delete_collected_outputs(self):
        log.info("discarding outputs of all collected jobs")
        self.__delete_collected_outputs_rec(self.jobs)
        log.info("all outputs discarded")

    def __delete_collected_outputs_rec(self, jobs_db):
        for job in reversed(list(jobs_db.values())):
            if isinstance(job, dict):
                self.__delete_collected_outputs_rec(job)
            else:
                log.info(f"discarding outputs of job {job.describe()}")
                self._discard_files(job.outputs)

    def _discard_files(self, files):
        discard_files(files, log)

    def execute_jobs(self, *, final=False):
        suffix = " (dry run)" if self.dry_run else ""

        if len(self.job_queue) > 0:
            self.__finalize_queue()
            if not self._collect_group:
                if final:
                    log.info("all jobs done" + suffix)
                else:
                    log.debug("flushed jobs" + suffix)
        else:
            if not self._collect_group:
                if final:
                    log.info("nothing to be done" + suffix)
                else:
                    log.debug("no jobs to be flushed" + suffix)

    def __finalize_queue(self):
        if self._collect_group:
            if len(self.job_queue) > 0:
                self._group_job_batches.append(self.job_queue)
                self.job_queue = []
            return

        local_jobs = [job for job in self.job_queue if job.exec_local]
        normal_jobs = [job for job in self.job_queue if not job.exec_local]

        try:
            if len(normal_jobs) > 0:
                self.executor(
                    normal_jobs,
                    dry_run=self.dry_run,
                    force=self.force,
                    print_commands=self.print_commands,
                    threads=self.threads,
                )
            if len(local_jobs) > 0:
                self.local_executor(
                    local_jobs,
                    dry_run=self.dry_run,
                    force=self.force,
                    print_commands=self.print_commands,
                    threads=self.threads,
                )
        except JobFailed as job_failure:
            self._discard_files(job_failure.job.outputs)
            raise job_failure

        for job in self.job_queue:
            assert job.state.is_done

        incomplete_outputs = [
            (job, self._check_outputs(job.inputs, job.outputs))
            for job in self.job_queue
        ]
        if any(len(jf[1]) > 0 for jf in incomplete_outputs):
            raise IncompleteOutputs(incomplete_outputs)

        # reset job queue
        self.job_queue = []

    def __execute_group_job_batches(self):
        assert self._collect_group
        assert not self.force or self.delete_outputs
        num_jobs = sum(len(job_batch) for job_batch in self._group_job_batches)

        if num_jobs == 0:
            log.warning("nothing to execute in grouped_jobs")
            return

        self.check_group_pre_conditions()
        if not self.check_group_post_conditions(return_bool=True):
            self._collect_group = False
            for job_batch in self._group_job_batches:
                self.job_queue = job_batch
                self.execute_jobs()
            self._collect_group = True
        else:
            log.debug(
                f"skipping group job `{self._group_job_name}`: "
                "all outputs are up-to-date"
            )
            if not self.force_delete_temp:
                # prevent modification of intermediate files
                self._group_job_batches = []

    def __clean_group_intermediates(self):
        assert self._collect_group
        assert not self.force

        if len(self._group_job_batches) == 0:
            return

        assert self.check_group_post_conditions(return_bool=True)

        all_files = chain(
            chain.from_iterable(
                job.inputs for job in chain.from_iterable(self._group_job_batches)
            ),
            chain.from_iterable(
                job.outputs for job in chain.from_iterable(self._group_job_batches)
            ),
        )
        all_files = set(str(file) for file in all_files)
        group_inputs = list(
            chain.from_iterable(job.inputs for job in self._group_job_batches[0])
        )
        group_outputs = list(
            chain.from_iterable(job.outputs for job in self._group_job_batches[-1])
        )
        interface_files = set(str(file) for file in chain(group_inputs, group_outputs))
        intermediate_files = [
            Path(intermediate_file) for intermediate_file in all_files - interface_files
        ]
        log.debug(f"interface_files=[{', '.join(interface_files)}]")
        log.debug(f"intermediate_files=[{', '.join(all_files - interface_files)}]")

        for intermediate_file in intermediate_files:
            if self.keep_temp:
                log.info(f"keeping temporary intermediate file `{intermediate_file}`")
            elif intermediate_file.exists():
                log.info(f"removing temporary intermediate file `{intermediate_file}`")
                intermediate_file.unlink()
            else:
                log.debug(
                    "no need to delete temporary intermediate "
                    f"file `{intermediate_file}`"
                )

    @contextmanager
    def grouped_jobs(
        self,
        group_name,
        temp_intermediates=False,
        pre_conditions=[],
        post_conditions=[],
    ):
        if self.force and not self.delete_outputs:
            # execute everything as normal if force
            yield
        else:
            with self.__collect_job_group(
                group_name,
                # disable temp_intermediates if delete_outputs
                temp_intermediates=temp_intermediates and not self.delete_outputs,
                pre_conditions=pre_conditions,
                post_conditions=post_conditions,
            ):
                yield

    @contextmanager
    def __collect_job_group(
        self,
        group_name,
        temp_intermediates=False,
        pre_conditions=[],
        post_conditions=[],
    ):
        self._collect_group = True
        self._group_job_batches = []
        self._group_job_name = group_name
        self._group_job_pre_conditions = pre_conditions
        self._group_job_pre_conditions.insert(0, self.check_grouped_jobs_preconditions)
        self._group_job_post_conditions = post_conditions
        self._group_job_post_conditions.append(self.is_group_up_to_date)
        try:
            yield
            self.execute_jobs(final=True)
            self.__execute_group_job_batches()
            if temp_intermediates:
                self.__clean_group_intermediates()
        finally:
            self._group_job_name = None
            self._group_job_batches = []
            self._collect_group = False

    def check_group_pre_conditions(self, return_bool=False):
        if return_bool:
            return not throws(self.check_group_pre_conditions)

        for handler in self._group_job_pre_conditions:
            inject(
                handler,
                name=self._group_job_name,
                first_stage=self._group_job_batches[0],
            )()

    def check_group_post_conditions(self, return_bool=False):
        if return_bool:
            return not throws(self.check_group_post_conditions)

        for handler in self._group_job_post_conditions:
            inject(
                handler,
                name=self._group_job_name,
                first_stage=self._group_job_batches[0],
                last_stage=self._group_job_batches[-1],
            )()

    @staticmethod
    def check_grouped_jobs_preconditions(first_stage):
        for job in first_stage:
            job.check_pre_conditions()

    @staticmethod
    def is_group_up_to_date(first_stage, last_stage):
        group_inputs = list(chain.from_iterable(job.inputs for job in first_stage))
        group_outputs = list(chain.from_iterable(job.outputs for job in last_stage))
        Workflow.check_up_to_date(group_inputs, group_outputs)


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
    def __init__(
        self,
        *,
        name,
        index=None,
        exec_local=False,
        inputs,
        outputs,
        action,
        log,
        resources,
        pre_conditions=[],
        post_conditions=[],
    ):
        self.name = name
        self.index = Job._check_index(index)
        self.exec_local = exec_local
        if action.local_only and not self.exec_local:
            raise ValueError("Must set `exec_local=True` for local-only action")
        self.inputs = inputs
        self.outputs = outputs
        if not isinstance(action, AbstractAction):
            raise ValueError("Job action must be derived from AbstractAction.")
        self.action = action
        self.log = log
        self.resources = resources
        self.threads = self.resources.get("threads", 1)
        self.state = JobState.WAITING
        self.exit_code = -1
        self.id = None
        self.pre_conditions = pre_conditions
        self.post_conditions = post_conditions

    @staticmethod
    def _check_index(index):
        if index is None or isinstance(index, int):
            return index
        elif isinstance(index, MultiIndex):
            return index
        else:
            try:
                return MultiIndex(*index)
            except TypeError:
                raise TypeError(
                    "Job index must be None, int or (convertible to) MultiIndex."
                )

    def check_pre_conditions(self, return_bool=False):
        if return_bool:
            return not throws(self.check_pre_conditions)

        for handler in self.pre_conditions:
            handler = inject(
                handler,
                job=self,
                name=self.name,
                index=self.index,
                inputs=self.inputs,
                outputs=self.outputs,
            )()

    def check_post_conditions(self, return_bool=False):
        if return_bool:
            return not throws(self.check_post_conditions)

        for handler in self.post_conditions:
            handler = inject(
                handler,
                job=self,
                name=self.name,
                index=self.index,
                inputs=self.inputs,
                outputs=self.outputs,
                state=self.state,
                exit_code=self.exit_code,
            )()

    @property
    def is_batch(self):
        return self.index is not None

    @property
    def output(self):
        if len(self.outputs) == 1:
            return next(iter(self.outputs))
        else:
            raise NotImplementedError(
                "`job.output` is only allowed if job has exactly one output"
                f"but got {len(self.outputs)}"
            )

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

    @contextmanager
    def open_log(self):
        if self.log is None:
            yield None
        else:
            with open(self.log, "w") as fd:
                yield fd

    def to_command(self):
        raise NotImplementedError(
            "Do not call job.to_command() directly; use job.action.to_command() instead"
        )

    def __str__(self):
        action_str = str(self.action)
        if self.log is None:
            return action_str
        elif "\n" in action_str:
            return f"{{\n{action_str}\n}} &> {self.log}"
        else:
            return f"{action_str} &> {self.log}"

    def describe(self):
        if self.id is None:
            return f"`{self.fullname}`"
        else:
            return f"`{self.fullname}` (id={self.id})"

    @property
    def fullname(self):
        if self.index is None:
            return self.name
        else:
            return f"{self.name}.{self.index}"

    @property
    def hash(self):
        return md5(bytes(self.fullname, "utf-8")).hexdigest()


class DuplicateJob(Exception):
    def __init__(self, existing, duplicate):
        super().__init__()
        self.existing = existing
        self.duplicate = duplicate

    def __str__(self):
        return f"duplicate job `{self.existing.describe()}`"


class FaultyFilesException(Exception):
    INDENT = "  "
    DESCRIPTION = "Faulty files"

    def __init__(self, job_files):
        super().__init__()
        self.job_files = job_files

    @property
    def jobs(self):
        return [jf[0] for jf in self.job_files]

    def __str__(self):
        file_indent = f"\n{2*self.INDENT}- "

        def file_list(files):
            return file_indent.join(str(f) for f in files)

        job_files = f"\n{self.INDENT}".join(
            f"{jf[0].describe()}:{file_indent}{file_list(jf[1])}"
            for jf in self.job_files
        )

        return f"{self.DESCRIPTION}:\n{self.INDENT}{job_files}"


class MissingInputs(FaultyFilesException):
    DESCRIPTION = "missing input file(s)"


class IncompleteOutputs(FaultyFilesException):
    DESCRIPTION = "missing or out-dated output file(s)"
