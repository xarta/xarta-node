"""Exercise the production renderer, apply transaction and actual Caddy routing."""

import http.server
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "setup-caddy.sh"


def run(*args, **kw):
    return subprocess.run(args, text=True, capture_output=True, **kw)


@pytest.fixture
def sandbox(tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=example.test",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    live = tmp_path / "Caddyfile"
    live.write_text("previous live bytes\n")
    link = tmp_path / "system-config"
    link.symlink_to(live)
    env = tmp_path / "input.env"
    values = dict(
        REPO_CADDY_PATH=str(tmp_path),
        REPO_NON_ROOT_PATH=str(tmp_path),
        REPO_INNER_PATH=str(tmp_path),
        BLUEPRINTS_FALLBACK_GUI_DIR=str(tmp_path / "ui"),
        BLUEPRINTS_DB_DIR=str(tmp_path / "db"),
        BLUEPRINTS_UI_URL="https://example.test",
        CERT_FILE=str(cert),
        CERT_KEY=str(key),
        SYNC_TLS_CA=str(cert),
        SYNC_TLS_CERT=str(cert),
        SYNC_TLS_KEY=str(key),
        CADDY_SYSTEM_CONFIG=str(link),
    )

    def configure(**updates):
        values.update(updates)
        import shlex

        env.write_text("".join(f"{k}={shlex.quote(v)}\n" for k, v in values.items()))
        return env

    configure()
    return tmp_path, env, live, link, configure


def render(sandbox, **values):
    directory, env, live, link, configure = sandbox
    configure(**values)
    output = directory / "candidate.caddy"
    output.unlink(missing_ok=True)
    result = run(
        "bash", str(SCRIPT), "--render-only", "--env-file", str(env), "--output", str(output)
    )
    return result, output


def test_render_is_isolated_and_mcp_disabled(sandbox):
    directory, env, live, link, configure = sandbox
    before = {p: p.read_bytes() for p in (env, live)}
    result, output = render(sandbox)
    assert result.returncode == 0, result.stderr
    assert "mcp_authenticated" not in output.read_text()
    assert all(p.read_bytes() == content for p, content in before.items())
    assert link.is_symlink()
    assert not (directory / "db").exists()


@pytest.mark.parametrize(
    "values",
    [
        dict(CADDY_MCP_ENABLED="true"),
        dict(CADDY_MCP_ENABLED="true", CADDY_MCP_PATH_PREFIX="/service/api/"),
        dict(CADDY_MCP_ENABLED="yes"),
        dict(
            CADDY_MCP_ENABLED="true",
            CADDY_MCP_PATH_PREFIX="/bad* /",
            CADDY_MCP_UPSTREAM="localhost:12345",
        ),
    ],
)
def test_incomplete_mcp_refused(sandbox, values):
    result, _ = render(sandbox, **values)
    assert result.returncode != 0


def test_complete_render_validates_and_contains_scoped_guards(sandbox):
    result, output = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM="localhost:12345",
        MODEL_HOOK_HTTP_HOST="127.0.0.1:18090",
        MODEL_HOOK_UPSTREAM="localhost:12346",
    )
    assert result.returncode == 0, result.stderr
    text = output.read_text()
    assert "handle /service/api/*" in text
    assert "{env.MCP_BEARER_TOKEN}" in text
    assert "reverse_proxy localhost:12345" in text
    block = text.split("http://127.0.0.1:18090 {", 1)[1].split("\n}\n", 1)[0]
    assert "respond 403" in block and "localhost:8080" not in block
    valid = run("caddy", "validate", "--adapter", "caddyfile", "--config", str(output))
    assert valid.returncode == 0, valid.stderr


def stub_services(directory):
    bindir = directory / "bin"
    bindir.mkdir()
    log = directory / "service-log"
    script = bindir / "systemctl"
    script.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "'
        + str(log)
        + '"\ncase "$*" in *--property=User*|*--property=Group*) echo root;; esac\n'
    )
    script.chmod(0o755)
    # No outbound request or delay is needed in an isolated apply test.
    for name, body in [("curl", "echo 200"), ("sleep", ":")]:
        f = bindir / name
        f.write_text("#!/bin/sh\n" + body + "\n")
        f.chmod(0o755)
    return dict(os.environ, PATH=str(bindir) + ":" + os.environ["PATH"]), log


def test_invalid_apply_preserves_live_symlink_metadata_and_service(sandbox):
    directory, env, live, link, configure = sandbox
    configure(BLUEPRINTS_UI_URL="https://invalid{host")
    process_env, log = stub_services(directory)
    before = live.read_bytes(), live.stat(), os.readlink(link), link.lstat()
    result = run("bash", str(SCRIPT), "--existing-only", "--env-file", str(env), env=process_env)
    assert result.returncode != 0
    assert live.read_bytes() == before[0]
    assert live.stat() == before[1]
    assert os.readlink(link) == before[2] and link.lstat() == before[3]
    assert "restart" not in log.read_text() and "enable caddy" not in log.read_text()
    assert not list(directory.glob(".Caddyfile.install.*"))


def test_successful_apply_installs_validated_bytes_and_preserves_metadata(sandbox):
    directory, env, live, link, configure = sandbox
    process_env, log = stub_services(directory)
    # Interpose only validation to retain the exact bytes it received.
    real = shutil.which("caddy")
    executable = directory / "bin" / "caddy"
    validated = directory / "validated"
    executable.write_text('#!/bin/sh\ncp "$5" "' + str(validated) + '"\nexec "' + real + '" "$@"\n')
    executable.chmod(0o755)
    live.chmod(0o640)
    os.setxattr(live, "user.caddy-test", b"preserved")
    if os.geteuid() == 0:
        os.chown(live, 1234, 1235)
    before = live.stat()
    link_before = link.lstat()
    result = run("bash", str(SCRIPT), "--existing-only", "--env-file", str(env), env=process_env)
    assert result.returncode == 0, result.stderr
    assert live.read_bytes() == validated.read_bytes()
    after = live.stat()
    assert (after.st_uid, after.st_gid, after.st_mode) == (
        before.st_uid,
        before.st_gid,
        before.st_mode,
    )
    assert os.getxattr(live, "user.caddy-test") == b"preserved"
    assert link.lstat() == link_before
    assert "restart caddy" in log.read_text()
    assert not list(directory.glob(".Caddyfile.install.*"))


@pytest.mark.parametrize("credential", [None, "", "synthetic-credential"])
def test_real_listener_auth_and_prefix_preservation(sandbox, credential):
    paths = []

    class Backend(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(self.path.encode())

        def log_message(self, *args):
            pass

    backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    thread = threading.Thread(target=backend.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    result, output = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM=f"127.0.0.1:{backend.server_port}",
    )
    assert result.returncode == 0, result.stderr
    # Use the actual generated auth block on a disposable plaintext listener.
    text = output.read_text()
    import importlib.util

    spec = importlib.util.spec_from_file_location("caddy_scope", ROOT / "check-caddy-security.py")
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    auth = checker.block(text, "handle /service/api/*")
    output.write_text("{\n admin off\n}\nhttp://127.0.0.1:" + str(port) + " {\n" + auth + "\n}\n")
    process_env = dict(os.environ)
    process_env.pop("MCP_BEARER_TOKEN", None)
    if credential is not None:
        process_env["MCP_BEARER_TOKEN"] = credential
    proc = subprocess.Popen(
        ["caddy", "run", "--adapter", "caddyfile", "--config", str(output)],
        env=process_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    def request(token):
        req = urllib.request.Request(f"http://127.0.0.1:{port}/service/api/read")
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, ""

    try:
        for _ in range(40):
            try:
                missing = request(None)
                break
            except OSError:
                time.sleep(0.05)
        else:
            pytest.fail("disposable Caddy did not start")
        assert missing[0] == 401
        assert request("")[0] == 401
        assert request("wrong")[0] == 401
        good = request("synthetic-credential")
        assert good[0] == (200 if credential else 401)
        if credential:
            assert good[1] == "/service/api/read" and paths == ["/service/api/read"]
        else:
            assert not paths
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        backend.shutdown()


@pytest.mark.parametrize("mutation", ["http-fallback", "missing-mcp", "unguarded-mcp"])
def test_security_parity_rejects_regressions(sandbox, mutation):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "caddy_security", ROOT / "check-caddy-security.py"
    )
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    result, output = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM="localhost:12345",
        MODEL_HOOK_HTTP_HOST="127.0.0.1:18090",
        MODEL_HOOK_UPSTREAM="localhost:12346",
    )
    assert result.returncode == 0
    text = output.read_text()
    if mutation == "http-fallback":
        text = text.replace("respond 403", "reverse_proxy localhost:8080")
    elif mutation == "missing-mcp":
        text = text.replace("handle /service/api/*", "handle /elsewhere/*")
    else:
        text = text.replace(
            'header Authorization "Bearer {env.MCP_BEARER_TOKEN}"', 'header X-Unrelated "value"'
        )
    with pytest.raises((AssertionError, ValueError)):
        checker.verify(
            text,
            ["127.0.0.1:18090", "localhost:12346", "true", "/service/api/", "localhost:12345"],
            generated=True,
        )


def test_render_never_calls_installers_or_services(sandbox):
    directory, env, live, link, configure = sandbox
    bindir = directory / "forbidden-bin"
    bindir.mkdir()
    log = directory / "forbidden-calls"
    for name in ("systemctl", "caddy", "apt-get", "gpg", "ln", "chown", "mkdir", "bash"):
        executable = bindir / name
        executable.write_text('#!/bin/sh\nprintf "%s\\n" "$0" >> "' + str(log) + '"\nexit 97\n')
        executable.chmod(0o755)
    output = directory / "render.caddy"
    result = run(
        "/bin/bash",
        str(SCRIPT),
        "--render-only",
        "--env-file",
        str(env),
        "--output",
        str(output),
        env=dict(os.environ, PATH=str(bindir) + ":" + os.environ["PATH"]),
    )
    assert result.returncode == 0, result.stderr
    assert not log.exists() and live.read_text() == "previous live bytes\n"


def test_private_operator_comments_survive_regeneration(sandbox):
    import json

    directory, env, live, link, configure = sandbox
    notes = {
        "mcp": "    # Keep the complete transport explanation.\n    # Uses handle (NOT handle_path) to preserve the configured prefix.\n    # The service drop-in supplies the runtime bearer credential.",
        "model_hook_http": "# PLAIN-HTTP ALLOWLIST: health and webhook only.\n# Everything else is 403; the full API remains on trusted HTTPS.",
    }
    file = directory / "operator-notes.json"
    file.write_text(json.dumps(notes))
    result, output = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM="localhost:12345",
        MODEL_HOOK_HTTP_HOST="127.0.0.1:18090",
        MODEL_HOOK_UPSTREAM="localhost:12346",
        CADDY_OPERATOR_NOTES_FILE=str(file),
    )
    assert result.returncode == 0, result.stderr
    generated = output.read_text()
    for section in notes.values():
        for line in section.splitlines():
            assert line.strip() in generated
    assert "handle /service/api/*" in generated
    assert "respond 403" in generated and "respond 401" in generated
    assert (
        run("caddy", "validate", "--adapter", "caddyfile", "--config", str(output)).returncode == 0
    )


def test_default_comments_explain_transport_and_allowlist(sandbox):
    result, output = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM="localhost:12345",
        MODEL_HOOK_HTTP_HOST="127.0.0.1:18090",
        MODEL_HOOK_UPSTREAM="localhost:12346",
    )
    assert result.returncode == 0, result.stderr
    generated = output.read_text()
    for explanation in (
        "Uses handle (NOT handle_path)",
        "caddy.service environment drop-in",
        "Missing, wrong or empty credentials",
        "PLAIN-HTTP ALLOWLIST",
        "Everything else on this block is 403",
        "NODE_EXTRA_CA_CERTS",
    ):
        assert explanation in generated


@pytest.mark.parametrize(
    "notes",
    [
        {"mcp": "reverse_proxy unexpected:1234"},
        {"unknown": "# unknown section"},
        {"mcp": ["# invalid value type"]},
    ],
)
def test_operator_notes_cannot_inject_configuration(sandbox, notes):
    import json

    directory, env, live, link, configure = sandbox
    file = directory / "operator-notes.json"
    file.write_text(json.dumps(notes))
    result, _ = render(
        sandbox,
        CADDY_MCP_ENABLED="true",
        CADDY_MCP_PATH_PREFIX="/service/api/",
        CADDY_MCP_UPSTREAM="localhost:12345",
        CADDY_OPERATOR_NOTES_FILE=str(file),
    )
    assert result.returncode != 0
    assert live.read_text() == "previous live bytes\n"


def test_render_does_not_refresh_repository_index(sandbox):
    directory, env, live, link, configure = sandbox
    subprocess.run(["git", "init", "-b", "main", str(directory)], check=True, capture_output=True)
    tracked = directory / "tracked.txt"
    tracked.write_text("unchanged content")
    subprocess.run(
        ["git", "-C", str(directory), "add", "tracked.txt"], check=True, capture_output=True
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(directory),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.test",
            "commit",
            "-m",
            "fixture",
        ],
        check=True,
        capture_output=True,
    )
    index = directory / ".git/index"
    tracked_stat = tracked.stat()
    os.utime(tracked, ns=(tracked_stat.st_atime_ns, tracked_stat.st_mtime_ns + 1000000000))
    before = index.read_bytes(), index.stat()
    result, _ = render(sandbox)
    assert result.returncode == 0, result.stderr
    assert index.read_bytes() == before[0]
    after = index.stat()
    assert (after.st_uid, after.st_gid, after.st_mode, after.st_ino, after.st_mtime_ns) == (
        before[1].st_uid,
        before[1].st_gid,
        before[1].st_mode,
        before[1].st_ino,
        before[1].st_mtime_ns,
    )
