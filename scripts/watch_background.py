"""Windowless CLI entry point with bounded, redacted lifecycle evidence."""

import importlib
import importlib.util
from pathlib import Path
import os
import json
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
# Load only the small stdlib evidence module before importing the CLI or the
# package. This keeps a later application import failure observable.
_spec = importlib.util.spec_from_file_location(
    "_campus_watch_runtime_evidence", ROOT / "campus_watch/runtime_evidence.py")
_evidence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_evidence)
record_runtime_event = _evidence.record_runtime_event


def _exit_code(value):
    if value is None:
        return 0
    if type(value) is int and -(2**31) <= value < 2**32:
        return value
    return 1


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        return 2
    journal = arguments[0]
    started_at = time.time()
    # An owned receipt lets the supervisor associate the venv redirector's
    # actual Python child even when it exits before the next process snapshot.
    receipt_name = os.environ.get("CAMPUS_WATCH_CHILD_RECEIPT")
    if receipt_name:
        try:
            receipt = Path(receipt_name)
            if receipt.parent.resolve() == Path(journal).parent.resolve() and receipt.name.endswith(".child.json"):
                allowed = not receipt.exists() or json.loads(receipt.read_text(encoding="utf-8")).get("schema") == "campus-watch-child-v1"
                if allowed:
                    temporary = receipt.with_suffix(".json.tmp")
                    temporary.write_text(json.dumps({"schema": "campus-watch-child-v1", "pid": os.getpid(),
                        "parent_pid": os.getppid(), "started_at": started_at}), encoding="utf-8")
                    temporary.replace(receipt)
        except Exception:
            pass
    record_runtime_event(journal, "process_started", started_at=started_at, stage="startup")
    stage = "import"
    error_type = None
    try:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        cli = importlib.import_module("campus_watch.__main__")
        stage = "cli"
        result = cli.main(arguments[1:])
        code = _exit_code(result)
        if type(result) is not int:
            code = 1
            error_type = "TypeError"
    except SystemExit as failure:
        code = _exit_code(failure.code)
        error_type = "SystemExit" if code else None
    except BaseException as failure:
        code = 130 if isinstance(failure, KeyboardInterrupt) else 1
        error_type = type(failure).__name__
        record_runtime_event(journal, "unhandled_exception", started_at=started_at,
                             stage=stage, error_type=error_type,
                             errno=getattr(failure, "errno", None),
                             winerror=getattr(failure, "winerror", None))
    facts = {"exit_code": code, "stage": "exit"}
    if error_type is not None:
        facts["error_type"] = error_type
    record_runtime_event(journal, "process_exit", started_at=started_at, **facts)
    return code


if __name__ == "__main__":
    sys.exit(main())
