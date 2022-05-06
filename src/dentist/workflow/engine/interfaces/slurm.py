from itertools import groupby
from subprocess import run, PIPE, Popen
import logging


default_params = {
    "time": "01:00:00",
    "mem-per-cpu": "1G",
}
log = logging.getLogger(__name__)
__solitary_job_template = """\
#!/bin/bash
{command}
"""
__batch_job_command_template = "{id}) {command} ;;"
__batch_job_template = """\
#!/bin/bash

if ! [[ -v SLURM_ARRAY_TASK_ID ]]
then
    echo "missing SLURM_ARRAY_TASK_ID" >&2
    exit 1
fi

case "$SLURM_ARRAY_TASK_ID" in
{commands}
*)
    echo "Unhandled job id: $SLURM_ARRAY_TASK_ID" >&2
    exit 1
    ;;
esac
"""


def submit_jobs(jobs, workdir, debug_flags):
    def job_spec(job):
        return (job.name, job.index)

    jobs.sort(key=job_spec)
    job_batches = list(list(g[1]) for g in groupby(jobs, key=job_spec))
    job_ids = list()
    debug = "slurm" in debug_flags
    for job_batch in job_batches:
        script_name = workdir.acquire_file(f"{job_batch[0].name}.sh")
        if len(job_batch) == 1 and not job_batch[0].is_batch:
            job_ids.append(_submit_solitary_job(job_batch[0], script_name, debug=debug))
        else:
            job_ids.extend(_submit_batch_job(job_batch, script_name, debug=debug))
    return job_ids


def _prepare_params(job_s):
    is_batch = isinstance(job_s, list)
    base_job = job_s[0] if is_batch else job_s
    params = base_job.resources
    if is_batch:
        params["array"] = ",".join(str(job.index) for job in job_s)

    return params.to_cli(tr={"ncpus": "c"})


def _submit_solitary_job(job, script_name, debug=False):
    command = str(job)
    params = _prepare_params(job)
    with open(script_name, "w") as job_script:
        job_script.write(__solitary_job_template.format(command=command))

    return _submit_script(script_name, params, debug)


def _submit_batch_job(jobs, script_name, debug=False):
    commands = [
        __batch_job_command_template.format(
            id=job.index,
            command=str(job).removeprefix(__default_shell_command),
        )
        for job in jobs
    ]
    params = _prepare_params(jobs)
    with open(script_name, "w") as job_script:
        job_script.write(__batch_job_template.format(commands="\n".join(commands)))

    slurm_id = _submit_script(script_name, params, debug)

    return [f"{slurm_id}.{job.index}" for job in jobs]


def _submit_script(script, params, debug=False):
    script = str(script)
    command = [
        "sbatch",
        "--parsable",
        *params,
        script,
    ]
    log.debug(f"submitting using {' '.join(command)}")

    if debug:
        Popen(["/bin/bash", script])

        return "DEBUG"
    else:
        proc = run(command, check=True, stdout=PIPE, text=True)

        return "/".join(proc.stdout.split(";"))
