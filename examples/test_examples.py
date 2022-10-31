import importlib.util
import logging
from pathlib import Path
from shutil import rmtree

logging.basicConfig(level=logging.DEBUG)


def _load_workflow_module(example_name):
    workflow_root = Path(__file__).parent / example_name
    spec = importlib.util.spec_from_file_location(
        example_name.replace("-", "_"), workflow_root / "workflow.py"
    )
    workflow_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(workflow_mod)

    return workflow_mod


def _get_workflow_args(example_name, select_args):
    workflow_root = Path(__file__).parent / example_name
    args = dict(
        indir=workflow_root / "inputs",
        outdir=workflow_root / "results",
        count=10,
    )
    wf_args = {"check_delay": 0.1}
    for arg in select_args:
        wf_args[arg] = args[arg]

    return wf_args


def _test_workflow(example_name, select_args):
    workflow_mod = _load_workflow_module(example_name)
    wf_args = _get_workflow_args(example_name, select_args)

    rmtree(wf_args["outdir"], ignore_errors=True)
    if hasattr(workflow_mod, "example_workflow"):
        workflow_mod.example_workflow(**wf_args)
    else:
        workflow_mod.ExampleWorkflow(**wf_args)()


def test_workflow_basic():
    _test_workflow("basic", {"indir", "outdir"})


def test_workflow_detached_execution():
    _test_workflow("detached-execution", {"count", "outdir"})


def test_workflow_group_jobs():
    _test_workflow("group-jobs", {"outdir"})


def test_workflow_inheritance():
    _test_workflow("inheritance", {"indir", "outdir"})


def test_workflow_parallel_test():
    _test_workflow("parallel-test", {"count", "outdir"})


def test_workflow_python_code():
    _test_workflow("python-code", {"indir", "outdir"})


def test_workflow_slurm():
    _test_workflow("slurm", {"count", "outdir"})


def test_workflow_file_lists():
    _test_workflow("file-lists", {"indir", "outdir"})
