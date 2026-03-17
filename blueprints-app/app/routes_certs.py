"""
routes_certs.py — TLS certificate management endpoints.

GET    /api/v1/certs/status   — status and details for all configured cert/key slots
POST   /api/v1/certs/upload   — write PEM content to the configured path for a slot

Security notes:
- Cert/key paths are resolved server-side from CERT_CONFIGS env-var lookup only.
  Clients never supply a filesystem path (prevents path-injection).
- Private key files are written with mode 0o600, certificate files with 0o644,
  both owned root:root.
- CA certs are automatically installed into the system trust store via
  update-ca-certificates after writing.
- Key material is NEVER returned by any endpoint (upload-only, no download).
- Content is never logged — only cert ids and paths are logged.
- Files go into CERTS_DIR (under the private inner repo, gitignored).
"""

import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg

log = logging.getLogger(__name__)

router = APIRouter(prefix="/certs", tags=["certs"])


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_path(cert_id: str) -> str:
    """
    Return the filesystem path for a cert slot.
    Uses the env var if set; otherwise falls back to CERTS_DIR + default_name.
    Raises HTTP 400 for unknown ids or if no path can be determined.
    """
    entry = cfg.CERT_CONFIGS.get(cert_id)
    if entry is None:
        raise HTTPException(status_code=400, detail=f"Unknown cert id: {cert_id!r}")
    path = os.environ.get(entry["env_var"], "")
    if path:
        return path
    if cfg.CERTS_DIR:
        return os.path.join(cfg.CERTS_DIR, entry["default_name"])
    raise HTTPException(
        status_code=400,
        detail=(
            f"Env var {entry['env_var']!r} is not set and CERTS_DIR is not configured. "
            "Set the env var in .env and restart the app."
        ),
    )


def _parse_cert_info(path: str) -> Optional[dict]:
    """
    Parse a PEM certificate file and return subject CN, expiry, and CA flag.
    Returns None if the file cannot be parsed.  Uses subprocess openssl.
    """
    try:
        r = subprocess.run(
            ["openssl", "x509", "-noout", "-subject", "-enddate", "-in", path],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode != 0:
            return None

        info: dict = {}
        for line in r.stdout.splitlines():
            if line.startswith("subject="):
                m = re.search(r'CN\s*=\s*([^,/\n]+)', line)
                info["cn"] = m.group(1).strip() if m else line.replace("subject=", "").strip()
            elif line.startswith("notAfter="):
                expires_str = line.replace("notAfter=", "").strip()
                try:
                    dt = datetime.strptime(expires_str, "%b %d %H:%M:%S %Y %Z").replace(
                        tzinfo=timezone.utc
                    )
                    info["expires"] = dt.strftime("%Y-%m-%d")
                    info["expires_days"] = (dt - datetime.now(tz=timezone.utc)).days
                except ValueError:
                    info["expires"] = expires_str
                    info["expires_days"] = None

        # Check CA flag separately (avoids loading full text for non-CA certs)
        ca_r = subprocess.run(
            ["openssl", "x509", "-noout", "-text", "-in", path],
            capture_output=True, text=True, timeout=5,
        )
        info["is_ca"] = "CA:TRUE" in ca_r.stdout
        return info
    except Exception:
        return None


def _safe_write(path: str, content: str, mode: int) -> None:
    """Atomically write PEM content to path and set ownership/permissions."""
    if not content.endswith("\n"):
        content += "\n"
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            f.write(content)
        os.chmod(tmp, mode)
        os.chown(tmp, 0, 0)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _install_ca(path: str) -> str:
    """
    Install a CA certificate into the system trust store (Debian/Ubuntu).
    Returns a human-readable status string.
    """
    ca_name = os.path.splitext(os.path.basename(path))[0]
    dest = f"/usr/local/share/ca-certificates/{ca_name}.crt"
    try:
        shutil.copy2(path, dest)
        r = subprocess.run(
            ["update-ca-certificates"],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return f"file copied but update-ca-certificates failed: {r.stderr.strip()}"
        added = re.search(r'\d+ added', r.stdout)
        extra = f" ({added.group(0)})" if added else ""
        return f"installed to system trust store{extra}"
    except Exception as exc:
        return f"trust store install failed: {exc}"


# ── Models ─────────────────────────────────────────────────────────────────────

class CertStatusItem(BaseModel):
    id: str
    label: str
    env_var: str
    path: str
    path_source: str        # "env_var" | "default" | "unconfigured"
    present: bool
    kind: str               # "cert" | "key" | "ca"
    group: str              # "caddy" | "mtls"
    description: str
    cn: Optional[str] = None
    expires: Optional[str] = None
    expires_days: Optional[int] = None
    is_ca: Optional[bool] = None


class CertsStatusOut(BaseModel):
    certs: list[CertStatusItem]
    certs_dir: str


class CertUploadIn(BaseModel):
    id: str
    pem: str


class CertUploadOut(BaseModel):
    id: str
    status: str                     # "written" | "failed"
    detail: Optional[str] = None
    ca_installed: Optional[str] = None  # non-None when CA was installed to trust store


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("/status", response_model=CertsStatusOut)
async def certs_status() -> CertsStatusOut:
    """Return presence and X.509 details for all configured certificate/key slots."""
    items: list[CertStatusItem] = []
    for cert_id, meta in cfg.CERT_CONFIGS.items():
        env_path = os.environ.get(meta["env_var"], "")
        if env_path:
            path = env_path
            path_source = "env_var"
        elif cfg.CERTS_DIR:
            path = os.path.join(cfg.CERTS_DIR, meta["default_name"])
            path_source = "default"
        else:
            path = "(not configured)"
            path_source = "unconfigured"

        present = bool(path and path != "(not configured)" and os.path.isfile(path))

        cert_info: Optional[dict] = None
        if present and meta["kind"] != "key":
            cert_info = _parse_cert_info(path)

        items.append(CertStatusItem(
            id=cert_id,
            label=meta["label"],
            env_var=meta["env_var"],
            path=path,
            path_source=path_source,
            present=present,
            kind=meta["kind"],
            group=meta["group"],
            description=meta["description"],
            cn=cert_info.get("cn") if cert_info else None,
            expires=cert_info.get("expires") if cert_info else None,
            expires_days=cert_info.get("expires_days") if cert_info else None,
            is_ca=cert_info.get("is_ca") if cert_info else None,
        ))
    return CertsStatusOut(certs=items, certs_dir=cfg.CERTS_DIR or "(not configured)")


@router.post("/upload", response_model=CertUploadOut)
async def cert_upload(body: CertUploadIn) -> CertUploadOut:
    """
    Write PEM cert/key content to the configured path for the given cert slot.
    CA-type slots are automatically installed into the system trust store.
    Upload only — no download endpoint exists by design.
    """
    if body.id not in cfg.CERT_CONFIGS:
        raise HTTPException(status_code=400, detail=f"Unknown cert id: {body.id!r}")

    meta = cfg.CERT_CONFIGS[body.id]
    pem = body.pem.strip()

    # Basic PEM sanity check
    if "-----BEGIN " not in pem:
        raise HTTPException(
            status_code=400,
            detail="Content does not look like PEM — expected '-----BEGIN ...' header.",
        )

    # Prevent cross-type uploads (e.g. cert pasted into a key slot)
    if meta["kind"] == "key":
        if "CERTIFICATE" in pem and "PRIVATE KEY" not in pem:
            raise HTTPException(
                status_code=400,
                detail="A certificate was provided but this slot requires a private key.",
            )
    elif meta["kind"] in ("cert", "ca"):
        if "PRIVATE KEY" in pem and "CERTIFICATE" not in pem:
            raise HTTPException(
                status_code=400,
                detail="A private key was provided but this slot requires a certificate.",
            )

    try:
        path = _resolve_path(body.id)
    except HTTPException as exc:
        return CertUploadOut(id=body.id, status="failed", detail=exc.detail)

    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        _safe_write(path, pem, meta["mode"])
        log.info("certs/upload: wrote %s id=%s to %s", meta["kind"], body.id, path)
    except Exception as exc:
        log.error("certs/upload: failed to write id=%s: %s", body.id, exc)
        return CertUploadOut(id=body.id, status="failed", detail=str(exc))

    # CA certs: install to system trust store automatically
    ca_msg: Optional[str] = None
    if meta["kind"] == "ca":
        ca_msg = _install_ca(path)
        log.info("certs/upload: CA trust store install for id=%s: %s", body.id, ca_msg)

    return CertUploadOut(id=body.id, status="written", ca_installed=ca_msg)
