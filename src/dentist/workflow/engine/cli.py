import argparse
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
    "dry_run": bool,
    "force": bool,
    "keep_temp": bool,
    "delete_temp": bool,
    "print_commands": bool,
    "threads": int,
    "resources": Path,
    "submit_jobs": str,
    "check_delay": float,
    "debug_flags": str,
}
_metavar = {}
_shortopts = {"dry_run": ["-n"]}
_choices = {"submit_jobs": interfaces.names}
_nargs = {}
_action = {"debug_flags": CollectSet}
_help = {
    "workflow_root": """
        Define root directory for the workflow under which
        --workflow-dir will be created.
    """,
    "workflow_dir": "Location of files that are required by the workflow engine.",
    "dry_run": "Just display what would be done but do not execute anything.",
    "force": "Unconditionally recreate files.",
    "keep_temp": "Do not delete temporary intermediate results.",
    "delete_temp": "Force deletion of temporary intermediate results.",
    "print_commands": "Print the commands that are executed.",
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


def cli_parser(script_root=None):
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
    override_defaults = {
        "workflow_root": script_root,
    }

    for param in workflow_params.values():
        long_opt = "--" + param.name.replace("_", "-")
        parser.add_argument(
            long_opt,
            *_shortopts.get(param.name, []),
            metavar=_metavar.get(param.name, None),
            type=_type.get(param.name, str),
            action=_action.get(param.name, None),
            default=override_defaults.get(param.name, param.default),
            choices=_choices.get(param.name, None),
            nargs=_nargs.get(param.name, None),
            help=_help.get(param.name, param.name.replace("_", " ")),
        )

    return parser
