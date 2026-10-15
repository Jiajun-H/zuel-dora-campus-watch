"""Windowless supervisor entry point; credentials remain environment-only."""

from pathlib import Path
import importlib.util
import os
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    journal = None
    try:
        index = arguments.index("--guard-state")
        journal = Path(arguments[index + 1]).with_suffix(".runtime.jsonl")
    except (ValueError, IndexError):
        pass
    spec = importlib.util.spec_from_file_location("_guard_runtime", ROOT / "campus_watch/runtime_evidence.py")
    evidence = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evidence)
    started_at = time.time()
    if journal:
        evidence.record_runtime_event(journal, "process_started", started_at=started_at, stage="startup")
    try:
        if not os.environ.get("BARK_KEY") and "--help" not in arguments:
            from campus_watch.local_secrets import load_bark_key
            key = load_bark_key(journal.parent / "bark_key.dpapi") if journal else None
            if not key:
                raise RuntimeError("Protected local notification key is unavailable.")
            os.environ["BARK_KEY"] = key
        from campus_watch.supervisor import main as run
        code = run(arguments)
    except BaseException as failure:
        if journal:
            evidence.record_runtime_event(journal, "unhandled_exception", started_at=started_at,
                stage="run", error_type=type(failure).__name__)
        code = 3
    if journal:
        evidence.record_runtime_event(journal, "process_exit", started_at=started_at, stage="exit", exit_code=code)
    return code


if __name__ == "__main__":
    code = main()
    sys.exit(code)
