"""Read-only CLI capability checks, before allocating cloud resources."""

import os
import re
import subprocess


def stop_command():
    """Choose stable or beta stop with the required graceful-period bypass."""
    diagnostics = []
    for prefix in ([], ["beta"]):
        command = ["gcloud", *prefix, "compute", "instances", "stop"]
        result = subprocess.run(
            [*command, "--help", "--quiet"],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "CLOUDSDK_PAGER": "cat", "PAGER": "cat", "TERM": "dumb"},
        )
        output = result.stdout + "\n" + result.stderr
        # man/terminal renderers can underline or bold with overstrike sequences.
        output = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", output)
        while "\b" in output:
            cleaned = re.sub(r"[^\n]\x08", "", output)
            if cleaned == output:
                break
            output = cleaned
        output = (
            output.replace("\u2010", "-").replace("\u2011", "-").replace("\u2212", "-")
        )
        if result.returncode == 0 and "--no-graceful-shutdown" in output:
            return command[1:]
        diagnostics.append(
            f"{' '.join(command)}: exit={result.returncode}\n{output[-1500:]}"
        )
    raise RuntimeError(
        "Neither stable nor beta gcloud stop help exposes --no-graceful-shutdown. "
        "Update Google Cloud CLI on the notebook host (and install its beta component "
        "if needed), then retry preparation on the current cluster. "
        "No stop was attempted. Help diagnostics:\n" + "\n".join(diagnostics)
    )
