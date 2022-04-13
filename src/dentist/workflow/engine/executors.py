from abc import ABC, abstractmethod
from itertools import chain
import logging
import subprocess

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
        raise NotImplemented("Define exeuction method.")


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


class LocalExecutor(AbstractExecutor):
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
        if print_commands:
            print(job)
        try:
            subprocess.run(job.to_command(), check=True)
            job.done()
            report_job(job)
        except subprocess.CalledProcessError as reason:
            job.failed()
            report_job(job)
            if return_error:
                return JobFailed(job, reason)
            else:
                raise JobFailed(job, reason)
