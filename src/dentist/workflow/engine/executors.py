from abc import ABC, abstractmethod
import subprocess


class AbstractExecutor(ABC):
    @abstractmethod
    def __call__(self, jobs, *, dry_run, print_commands, threads):
        raise NotImplemented("Define exeuction method.")


class JobFailed(Exception):
    def __init__(self, job, reason):
        super().__init__()
        self.job = job
        self.reason = reason

    def __str__(self):
        return f"job `{self.job.name}` failed: {self.reason}"


class LocalExecutor(AbstractExecutor):
    def __call__(self, jobs, *, dry_run, print_commands, threads=1):
        if dry_run:
            self._dry_run(jobs, print_commands=print_commands)
        elif threads > 1 and len(jobs) > 1:
            self._run_parallel(jobs, print_commands=print_commands, threads=threads)
        else:
            self._run_serial(jobs, print_commands=print_commands)

    def _dry_run(self, jobs, *, print_commands):
        if print_commands:
            for job in jobs:
                print(job)

    def _run_serial(self, jobs, *, print_commands):
        for job in jobs:
            LocalExecutor._execute_job(job, print_commands=print_commands)

    def _run_parallel(self, jobs, *, print_commands, threads):
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=threads) as pool:
            pool.map(
                lambda job: LocalExecutor._execute_job(
                    job, print_commands=print_commands
                ),
                jobs,
            )

    @staticmethod
    def _execute_job(job, *, print_commands):
        if print_commands:
            print(job)
        try:
            subprocess.run(job.to_command(), check=True)
        except subprocess.CalledProcessError as reason:
            raise JobFailed(job, reason)
