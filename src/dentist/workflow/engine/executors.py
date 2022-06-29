import logging
import subprocess
from abc import ABC, abstractmethod
from inspect import signature
from itertools import chain
from time import sleep

log = logging.getLogger(__name__)


def report_job(job):
    if job.state.is_waiting:
        log.info(f"waiting for job {job.describe()}.")
    elif job.state.is_done:
        log.info(f"job {job.describe()} done.")
    else:
        log.error(f"job {job.describe()} FAILED.")


class AbstractExecutor(ABC):
    requires_status_tracking = False

    def __call__(self, jobs, *, dry_run, print_commands, threads):
        if dry_run:
            self._dry_run(jobs, print_commands=print_commands)
        else:
            self._run_jobs(jobs, print_commands=print_commands, threads=threads)

    def _dry_run(self, jobs, *, print_commands):
        if print_commands:
            for job in jobs:
                job.done()
                print(job)

    @abstractmethod
    def _run_jobs(self, jobs, *, print_commands, threads):
        raise NotImplementedError("Define exeuction method.")


class JobFailure(Exception):
    def __init__(self, jobs, reason):
        super().__init__()
        self.jobs = jobs
        self.reason = reason

    def outputs(self):
        return list(chain.from_iterable(job.outputs for job in self.jobs))

    def __str__(self):
        return f"{len(self.jobs)} job(s) failed: {self.reason}"


class JobFailed(JobFailure):
    def __init__(self, job, reason):
        super().__init__([job], reason)

    @property
    def job(self):
        return self.jobs[0]

    def outputs(self):
        return self.job.outputs

    def __str__(self):
        return f"job {self.job.describe()} failed: {self.reason}"


class JobBatchFailed(JobFailure):
    def __init__(self, job_failures, total_jobs):
        combined_reason = "\n".join(str(e) for e in job_failures)
        super().__init__([e.job for e in job_failures], combined_reason)
        self.total_jobs = total_jobs

    def __str__(self):
        return (
            f"{len(self.jobs)} of {self.total_jobs} batch job(s) failed:\n{self.reason}"
        )


class DetachedJobsFailed(JobFailure):
    def __init__(self, jobs, total_jobs):
        super().__init__(jobs, "unspecified error")
        self.total_jobs = total_jobs

    def __str__(self):
        job_specs = "\n".join(job.describe() for job in self.jobs)

        return (
            f"{len(self.jobs)} of {self.total_jobs} detached job(s) failed:\n"
            f"{job_specs}\n"
            "Check log files for details."
        )


class LocalExecutor(AbstractExecutor):
    def __init__(self, *, optargs=dict()):
        self.workdir = optargs.get("workdir", None)
        self.debug_flags = optargs.get("debug_flags", set())

    def _run_jobs(self, jobs, *, print_commands, threads=1):
        if threads == 1 and len(jobs) <= 1:
            self._run_serial(jobs, print_commands=print_commands)
        else:
            self._run_parallel(jobs, print_commands=print_commands, threads=threads)

    def _run_serial(self, jobs, *, print_commands):
        for job in jobs:
            LocalExecutor._execute_job(job, print_commands=print_commands)

    def _run_parallel(self, jobs, *, print_commands, threads):
        from concurrent.futures import ThreadPoolExecutor

        errors = None
        with ThreadPoolExecutor(max_workers=threads) as pool:
            errors = pool.map(
                lambda job: LocalExecutor._execute_job(
                    job,
                    print_commands=print_commands,
                    return_error=True,
                ),
                jobs,
            )

        errors = [e for e in errors if e is not None]
        if len(errors) > 0:
            raise JobBatchFailed(errors, len(jobs))

    @staticmethod
    def _execute_job(job, *, print_commands, return_error=False):
        if job.resources["ncpus"] > 1:
            log.warning(
                "unsupported operation for local execution: "
                f"job `{job.describe()}` requested {job.resources['ncpus']} CPUs."
            )

        if print_commands:
            print(job)

        if callable(job.action):
            try:
                job.action()
                job.done()
                report_job(job)
            except Exception as reason:
                job.failed(1)
                with job.open_log() as log_fp:
                    if log_fp is not None:
                        log_fp.write(str(reason))
                        log_fp.write("\n")
                report_job(job)
                if return_error:
                    return JobFailed(job, reason)
                else:
                    raise JobFailed(job, reason)
        else:
            try:
                with job.open_log() as log_fp:
                    subprocess.run(
                        job.to_command(), check=True, stdout=log_fp, stderr=log_fp
                    )
                job.done()
                report_job(job)
            except subprocess.CalledProcessError as reason:
                job.failed(reason.returncode)
                report_job(job)
                if return_error:
                    return JobFailed(job, reason)
                else:
                    raise JobFailed(job, reason)


class DetachedExecutor(AbstractExecutor):
    requires_status_tracking = True

    def __init__(self, *, submit_jobs, check_delay=15, optargs=dict()):
        self.submit_jobs = submit_jobs
        self.check_delay = check_delay
        self.optargs = optargs

    def _run_jobs(self, jobs, *, print_commands, threads=1):
        self._submit_jobs(jobs, print_commands=print_commands)
        self._wait_for_jobs(jobs)

    def _print_jobs(self, jobs):
        for job in jobs:
            print(job)

    def _submit_jobs(self, jobs, *, print_commands):
        self._print_jobs(jobs)
        submit_params = signature(self.submit_jobs).parameters
        submit_args = dict()
        for key, value in self.optargs.items():
            if key in submit_params:
                submit_args[key] = value
        job_ids = self.submit_jobs(jobs, **submit_args)
        assert len(jobs) == len(job_ids)
        for id, job in zip(job_ids, jobs):
            job.id = id

    def _wait_for_jobs(self, jobs):
        num_finished = 0
        while num_finished < len(jobs):
            sleep(self.check_delay)

            # update job tracking and issue messages
            # breakpoint()
            for job in jobs:
                if job.state.is_waiting:
                    status = job.get_status()
                    if status >= 0:
                        num_finished += 1
                        if status == 0:
                            job.done()
                            log.info(f"job {job.describe()} done.")
                        else:
                            job.failed(status)
                            log.error(f"job {job.describe()} FAILED.")
                    else:
                        log.debug(f"waiting for job {job.describe()}...")

        # raise exception upon failure
        failed = [job for job in jobs if job.state.is_failed]
        if len(failed) > 0:
            raise DetachedJobsFailed(failed, len(jobs))
