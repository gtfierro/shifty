"""Publish the workspace crates, resuming partial releases and rate limits."""

import json
import re
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

PACKAGES = (
    "shifty-algebra",
    "shifty-parse",
    "shifty-repair",
    "shifty-opt",
    "shifty-engine",
    "shifty-vio",
    "shapeshift-macro",
    "shapeshift-parse",
    "shapeshift-import",
    "tree-sitter-shapeshift",
    "shapeshift-raise",
    "shifty-shapeshift",
    "shifty-experimental",
    "shifty-cli",
)


def is_published(package, version):
    # Every workspace package name is at least four characters long.
    url = f"https://index.crates.io/{package[:2]}/{package[2:4]}/{package}"
    try:
        with urlopen(url, timeout=60) as response:
            rows = response.read().decode().splitlines()
    except HTTPError as error:
        if error.code == 404:
            return False
        raise
    return any(json.loads(row)["vers"] == version for row in rows)


def retry_delay(output):
    if "429 Too Many Requests" not in output:
        return None
    match = re.search(r"Please try again after (.+? GMT)", output)
    if match is None:
        return None
    resume = parsedate_to_datetime(match[1])
    return max(5, int((resume - datetime.now(timezone.utc)).total_seconds()) + 5)


def publish(root):
    version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"][
        "version"
    ]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError(f"Expected a stable release version, got {version}")
    for package in PACKAGES:
        if is_published(package, version):
            print(f"Skipping published {package} {version}", flush=True)
            continue
        while True:
            result = subprocess.run(
                ["cargo", "publish", "--locked", "-p", package],
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=None,
            )
            print(result.stdout, end="", flush=True)
            if result.returncode == 0:
                break
            delay = retry_delay(result.stdout)
            if delay is None:
                raise subprocess.CalledProcessError(result.returncode, result.args)
            print(f"Registry rate limit; retrying {package} in {delay}s", flush=True)
            time.sleep(delay)


if __name__ == "__main__":
    publish(Path(sys.argv[1]).resolve())
