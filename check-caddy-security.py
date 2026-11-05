#!/usr/bin/env python3
"""Scoped security parity against the actual production renderer."""

import argparse
import re
import subprocess
import tempfile
from pathlib import Path


def block(text, header):
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.strip() == header + " {":
            selected, depth = [], 0
            for line in lines[i:]:
                line = line.split("#", 1)[0].strip()
                depth += line.count("{") - line.count("}")
                selected.append(line)
                if depth == 0:
                    return "\n".join(selected)
    raise ValueError("Missing scoped route: " + header)


def verify(text, settings, generated=False):
    host, hook, enabled, prefix, upstream = settings
    if host and hook:
        scoped = block(text, "http://" + host)
        for path in ("/xarta/health*", "/xarta/webhook*"):
            child = block(scoped, "handle " + path)
            assert child == f"handle {path} {{\nreverse_proxy {hook}\n}}", (
                "HTTP allowlist backend changed"
            )
            scoped = scoped.replace(child, "")
        assert scoped.replace("\n", "") == f"http://{host} {{respond 403}}", (
            "HTTP default must deny"
        )
    if enabled == "true":
        scoped = block(text, "handle " + prefix + "*")
        assert "handle_path" not in scoped, "MCP must preserve prefix"
        matcher = re.search(
            r'@([A-Za-z0-9_]+)(?:\s*\{|\s+)\s*header Authorization "Bearer \{env.MCP_BEARER_TOKEN\}"',
            scoped,
        )
        assert matcher, "MCP bearer matcher missing"
        proxy = block(scoped, "handle @" + matcher.group(1))
        assert f"reverse_proxy {upstream}" in proxy, "MCP authenticated backend changed"
        assert "respond 401" in scoped and scoped.count("reverse_proxy") == 1, (
            "MCP denial missing or unguarded backend"
        )
        if generated:
            assert 'expression `{env.MCP_BEARER_TOKEN} != ""`' in scoped, (
                "MCP empty credential guard missing"
            )
    elif generated:
        assert "mcp_authenticated" not in text, "MCP must default to disabled"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("setup", "active", "env"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    shell = 'source "$1"; printf "%s\\0" "${MODEL_HOOK_HTTP_HOST:-}" "${MODEL_HOOK_UPSTREAM:-}" "${CADDY_MCP_ENABLED:-false}" "${CADDY_MCP_PATH_PREFIX:-}" "${CADDY_MCP_UPSTREAM:-}"'
    settings = (
        subprocess.check_output(["bash", "-c", shell, "bash", args.env]).decode().split("\0")[:5]
    )
    with tempfile.TemporaryDirectory(prefix="caddy-parity-") as directory:
        output = Path(directory) / "candidate.caddy"
        subprocess.run(
            ["bash", args.setup, "--render-only", "--env-file", args.env, "--output", str(output)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        verify(output.read_text(), settings, generated=True)
        verify(Path(args.active).read_text(), settings)
    print("[caddy-security-parity] OK")


if __name__ == "__main__":
    main()
