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

    def __call__(self, jobs, *, dry_run, force, print_commands, threads):
        if dry_run:
            self._dry_run(jobs, print_commands=print_commands)
        else:
            self._run_jobs(
                jobs, force=force, print_commands=print_commands, threads=threads
            )

    def _dry_run(self, jobs, *, print_commands):
        if print_commands:
            for job in jobs:
                job.done()
                print(job)

    @abstractmethod
    def _run_jobs(self, jobs, *, force, print_commands, threads):
        raise NotImplementedError("Define exeuction method.")


class JobFailure(Exception):
    def __init__(self, jobs, reason):
        super().__init__()
        self.jobs = jobs
        self.reason = reason

    def outputs(self):
        return list(chain.from_iterable(job.outputs for job in self.jobs))

    def logs(self):
        return [job.log for job in self.jobs if job.log is not None]

    def has_logs(self):
        return len(self.logs()) > 0

    def _logs_notice(self):
        notice = ""
        if self.has_logs():
            notice = "\n- ".join(str(log) for log in self.logs())
            notice = f"\n\nCheck these log files:\n- {notice}"

        return notice

    def __str__(self):
        return f"{len(self.jobs)} job(s) failed: {self.reason}{self._logs_notice()}"


class JobFailed(JobFailure):
    def __init__(self, job, reason):
        super().__init__([job], reason)

    @property
    def job(self):
        return self.jobs[0]

    def outputs(self):
        return self.job.outputs

    def log(self):
        return self.job.log

    def _log_notice(self):
        if not self.has_logs():
            return ""

        return f"\n\nCheck this log file: {self.log()}"

    def __str__(self):
        return f"job {self.job.describe()} failed: {self.reason}{self._log_notice()}"


class JobBatchFailed(JobFailure):
    def __init__(self, job_failures, total_jobs):
        combined_reason = "\n".join(str(e) for e in job_failures)
        super().__init__([e.job for e in job_failures], combined_reason)
        self.total_jobs = total_jobs

    def __str__(self):
        return (
            f"{len(self.jobs)} of {self.total_jobs} batch job(s) failed:\n"
            f"{self.reason}{self._logs_notice()}"
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

    def _run_jobs(self, jobs, *, force, print_commands, threads=1):
        if threads == 1 and len(jobs) <= 1:
            self._run_serial(jobs, force=force, print_commands=print_commands)
        else:
            self._run_parallel(
                jobs, force=force, print_commands=print_commands, threads=threads
            )

    def _run_serial(self, jobs, *, force, print_commands):
        for job in jobs:
            LocalExecutor._execute_job(job, force=force, print_commands=print_commands)

    def _run_parallel(self, jobs, *, force, print_commands, threads):
        from concurrent.futures import ThreadPoolExecutor

        for job in jobs:
            if job.threads > threads:
                raise JobFailed(
                    job,
                    "insuffient number of threads provided: "
                    f"got {threads} but job needs {job.threads}",
                )

        job_queue = jobs.copy()
        available_threads = threads
        errors = list()

        def job_finished_cb(future):
            nonlocal available_threads, errors

            assert future.done()
            result = future.result()

            if isinstance(result, Exception):
                if isinstance(result, JobFailed):
                    available_threads += result.job.threads
                    errors.append(result)
                else:
                    raise result
            else:
                available_threads += result.threads

        with ThreadPoolExecutor(max_workers=threads) as pool:
            while len(job_queue) > 0:
                submitted_jobs = list()
                for job in job_queue:
                    if job.threads <= available_threads:
                        available_threads -= job.threads
                        future = pool.submit(
                            LocalExecutor._execute_job,
                            job,
                            force=force,
                            print_commands=print_commands,
                            return_error=True,
                        )
                        future.add_done_callback(job_finished_cb)
                        submitted_jobs.append(job)
                # remove submitted jobs from the queue
                job_queue = [job for job in job_queue if job not in submitted_jobs]
                if len(job_queue) == 0:
                    # all jobs submitted -> just wait for results
                    break
                # wait a bit before submitting more jobs
                sleep(0.1)

        errors = [e for e in errors if e is not None]
        if len(errors) > 0:
            raise JobBatchFailed(errors, len(jobs))

    @staticmethod
    def _execute_job(job, *, force, print_commands, return_error=False):
        if print_commands:
            print(job)

        if force:
            # delete outputs before running the command again
            for output in job.outputs:
                output.unlink(missing_ok=True)

        if callable(job.action):
            try:
                job.action()
                job.done()
                report_job(job)

                return job
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

                return job
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
