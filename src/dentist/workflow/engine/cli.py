import argparse
import logging
import sys
from inspect import signature
from pathlib import Path

from . import interfaces
from .workflow import Workflow


class CollectSet(argparse.Action):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not hasattr(namespace, self.dest):
            setattr(namespace, self.dest, set())
        set_ = getattr(namespace, self.dest)
        if isinstance(values, str):
            set_.add(values)
        else:
            set_.update(iter(values))


_skip_cli = [
    "self",
    "definition",
    "executor",
    "local_executor",
]
_type = {
    "workflow_root": Path,
    "workflow_dir": Path,
    "targets": str,
    "dry_run": bool,
    "force": bool,
    "keep_temp": bool,
    "force_delete_temp": bool,
    "print_commands": bool,
    "touch": bool,
    "delete_outputs": bool,
    "threads": int,
    "resources": Path,
    "submit_jobs": str,
    "check_delay": float,
    "debug_flags": str,
}
_metavar = {}
_shortopts = {
    "targets": ["-T"],
    "dry_run": ["-n"],
    "touch": ["-t"],
}
_choices = {"submit_jobs": interfaces.names}
_nargs = {}
_action = {
    bool: "store_true",
    "targets": CollectSet,
    "debug_flags": CollectSet,
}
_help = {
    "workflow_root": """
        Define root directory for the workflow under which
        --workflow-dir will be created.
    """,
    "workflow_dir": "Location of files that are required by the workflow engine.",
    "targets": """
        Run workflow until all target jobs have been executed – successfully or not.
    """,
    "dry_run": "Just display what would be done but do not execute anything.",
    "force": "Unconditionally recreate files.",
    "keep_temp": "Do not delete temporary intermediate results.",
    "force_delete_temp": """
        Force deletion of temporary intermediate results.
        This is useful after a run with --keep-temp because temporary
        intermediate results are not deleted when the group job is skipped.
    """,
    "print_commands": "Print the commands that are executed.",
    "touch": """
        Touch files (mark them up to date without really changing them)
        instead of running their commands. This is used to pretend that the
        commands were done. Note, new files will not be created!
    """,
    "delete_outputs": """
        Deletes all outputs that were collected during a forced dry run.
        Note, this may not clean up all generated files.
        Implies --dry-run and --force.
    """,
    "threads": "Number of threads to use in local execution.",
    "resources": """
        YAML or JSON file that specifies resources for jobs. Resources are
        passed to the executor, e.g. to allocate resources
        in a cluster environment.
    """,
    "submit_jobs": "Submit jobs for detached execution, e.g. on a cluster",
    "check_delay": "Check status of detached jobs every CHECK_DELAY seconds",
    "debug_flags": "Activate specific debugging facilities.",
}


class LogLevel(int):
    _to_log_level = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }

    def __new__(cls, level):
        obj = int.__new__(cls, LogLevel._to_log_level.get(level, level))

        return obj

    def __eq__(self, other):
        if isinstance(other, int):
            return super().__eq__(other)
        elif isinstance(other, str):
            return other in LogLevel._to_log_level and self == LogLevel(other)
        else:
            return False


LogLevel("critical")


def cli_parser(script_root=None, log_level=False, **override_defaults):
    if script_root is None:
        script_root = Path(sys.argv[0]).parent

    parser = argparse.ArgumentParser(
        add_help=True,
        epilog="""
            Powered by the DENTIST workflow engine.
            Copyright © 2022 Arne Ludwig <arne.ludwig@posteo.de>
        """,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    workflow_params = dict(signature(Workflow.__init__).parameters)
    for param in _skip_cli:
        del workflow_params[param]

    if "workflow_root" not in override_defaults:
        override_defaults["workflow_root"] = script_root

    for param in workflow_params.values():
        long_opt = "--" + param.name.replace("_", "-")
        type_ = _type.get(param.name, str)
        kwargs = dict(
            type=type_,
            action=_action.get(param.name, _action.get(type_, None)),
            default=override_defaults.get(param.name, param.default),
            choices=_choices.get(param.name, None),
            nargs=_nargs.get(param.name, None),
            help=_help.get(param.name, param.name.replace("_", " ")),
            metavar=_metavar.get(param.name, None),
        )
        if kwargs["action"] in {"store_const", "store_true", "store_false"}:
            del kwargs["metavar"]
            del kwargs["type"]
        kwargs = dict((k, v) for k, v in kwargs.items() if v is not None)
        parser.add_argument(long_opt, *_shortopts.get(param.name, []), **kwargs)

    if log_level:
        parser.add_argument(
            "--log-level",
            "-L",
            type=LogLevel,
            choices=("critical", "error", "warning", "info", "debug"),
            default="info",
            help="set logging verbosity",
        )

    return parser
