from abc import ABC, abstractmethod
import subprocess


class AbstractExecutor(ABC):
    @abstractmethod
    def __call__(self, jobs, *, dry_run, print_commands):
        raise NotImplemented("Define exeuction method.")


class LocalExecutor(AbstractExecutor):
    def __call__(self, jobs, *, dry_run, print_commands):
        if dry_run:
            if print_commands:
                for job in jobs:
                    print(job)
        else:
            for job in jobs:
                if print_commands:
                    print(job)
                try:
                    subprocess.run(job.to_command(), check=True)
                except subprocess.CalledProcessError as reason:
                    raise JobFailed(job, reason)


class JobFailed(Exception):
    def __init__(self, job, reason):
        super().__init__()
        self.job = job
        self.reason = reason

    def __str__(self):
        return f"job `{self.job.name}` failed: {self.reason}"
