"""Create a windowless supervisor via the Windows process provider.

Only a public command line crosses the provider boundary. Credentials are
loaded by the supervisor from CurrentUser DPAPI; caller environment is not
assumed to survive an independent Windows launch.
"""

import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace


class IndependentLaunchError(RuntimeError):
    pass


def spawn_independent(command, *, cwd, script):
    if os.name != "nt":
        raise IndependentLaunchError("Independent Windows launch is unavailable.")
    arguments = list(command)
    binary = Path(arguments[0]).with_name("pythonw.exe")
    if not binary.is_file():
        raise IndependentLaunchError("Windowless interpreter is unavailable.")
    arguments[0] = str(binary)
    shell = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    failed = False
    response = None
    try:
        result = subprocess.run([str(shell), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)],
            input=json.dumps({"command": subprocess.list2cmdline(arguments), "cwd": str(cwd)}).encode("utf-8"),
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=None,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=20)
        response = json.loads(result.stdout.decode("utf-8-sig"))
        if result.returncode or type(response) is not dict or response.get("return_code") != 0:
            failed = True
        elif type(response.get("pid")) is not int or not 0 < response["pid"] < 2**31:
            failed = True
    except Exception:
        failed = True
    if failed:
        raise IndependentLaunchError("Independent supervisor launch failed; no inherited fallback was started.")
    return SimpleNamespace(pid=response["pid"])
