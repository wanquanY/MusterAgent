#!/usr/bin/env python3
"""Build labelled acceptance images. Proxy configuration is environment-only."""
import datetime
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, urlunsplit

root = Path(__file__).resolve().parent.parent
evidence = Path(os.environ.get("P6_EVIDENCE", ".codex-work/p6")).resolve()
evidence.mkdir(parents=True, exist_ok=True)
proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
build_args = []
if proxy:
    parsed = urlsplit(proxy)
    if parsed.username or parsed.password:
        raise SystemExit("The test image builder requires a proxy without URL credentials")
    if parsed.hostname in ("localhost", "127.0.0.1", "::1"):
        parsed = parsed._replace(netloc="host.docker.internal:" + str(parsed.port or 80))
    translated = urlunsplit(parsed)
    build_args = ["--build-arg", "HTTP_PROXY=" + translated,
                  "--build-arg", "HTTPS_PROXY=" + translated]
labels = []
for key, value in {"owner": "musteragent", "run-id": "p6",
                   "purpose": "sandbox-acceptance", "disposable": "true",
                   "expires-at": (datetime.datetime.now(datetime.timezone.utc)
                                  + datetime.timedelta(days=2)).isoformat()}.items():
    labels += ["--label", key + "=" + value]
images = []
for dockerfile, tag in [("Dockerfile.example", "musteragent-sandbox-example:p6"),
                        ("Dockerfile.mcp-test", "musteragent-sandbox-mcp:p6")]:
    with (evidence / (dockerfile + ".build.log")).open("a") as log:
        subprocess.run(["docker", "build", "--progress=plain", *build_args, *labels,
                        "-f", str(root / "docker" / dockerfile), "-t", tag,
                        str(root)], stdout=log, stderr=subprocess.STDOUT, check=True)
    info = json.loads(subprocess.check_output(["docker", "image", "inspect", tag]))[0]
    images.append({"reference": tag, "id": info["Id"], "size": info["Size"],
                   "labels": info["Config"].get("Labels")})
(evidence / "built-images.json").write_text(json.dumps(images, indent=2) + "\n")
print("Built labelled Python/uv and Node acceptance images")
