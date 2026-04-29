"""
routes_keys.py — SSH probe key management endpoints.

GET    /api/v1/keys/status          — which key files exist on this node
POST   /api/v1/keys/import          — write key files from supplied material
DELETE /api/v1/keys/{id}            — delete private + public key files
GET    /api/v1/keys/store           — return encrypted key store blob (ciphertext only)
PUT    /api/v1/keys/store           — persist encrypted key store blob (ciphertext only)

Security notes:
- Key paths are resolved server-side from KEY_CONFIGS env-var lookup only.
  Clients never supply a filesystem path (prevents path-injection).
- Private key files are written with mode 0o600, public with 0o644, both
  owned root:root.
- Key material is NEVER logged — only key ids are logged.
- Unknown key ids are rejected with HTTP 400.
"""

import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg

log = logging.getLogger(__name__)


router = APIRouter(prefix="/keys", tags=["keys"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_path(key_id: str) -> str:
    """
    Return the filesystem path for a key id by reading the env var declared
    in KEY_CONFIGS.  Raises HTTP 400 for unknown ids or unconfigured paths.
    """
    entry = cfg.KEY_CONFIGS.get(key_id)
    if entry is None:
        raise HTTPException(status_code=400, detail=f"Unknown key id: {key_id!r}")
    path = os.environ.get(entry["env_var"], "")
    if not path:
        raise HTTPException(
            status_code=400,
            detail=f"Env var {entry['env_var']!r} is not set on this node.",
        )
    return path


def _read_pub_comment(pub_path: str) -> Optional[str]:
    """Extract the comment field (last token) from the first line of a .pub file."""
    try:
        with open(pub_path) as f:
            first = f.readline().strip()
        parts = first.split()
        return parts[-1] if len(parts) >= 3 else None
    except Exception:
        return None


def _safe_write(path: str, content: str, mode: int) -> None:
    """Atomically write content to path and set permissions."""
    # SSH requires key files to end with a newline.
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


# ── Models ────────────────────────────────────────────────────────────────────

class KeyStatusItem(BaseModel):
    id: str
    label: str
    env_var: str
    path: str
    present: bool
    pub_present: bool
    comment: Optional[str] = None


class KeysStatusOut(BaseModel):
    keys: list[KeyStatusItem]


class KeyImportItem(BaseModel):
    id: str
    private: str
    public: str


class KeysImportIn(BaseModel):
    keys: list[KeyImportItem]


class KeyImportResult(BaseModel):
    id: str
    status: str          # "written" | "skipped" | "failed"
    detail: Optional[str] = None


class KeysImportOut(BaseModel):
    results: list[KeyImportResult]


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/status", response_model=KeysStatusOut)
async def keys_status() -> KeysStatusOut:
    """Return presence status for all known SSH key files."""
    items: list[KeyStatusItem] = []
    for key_id, meta in cfg.KEY_CONFIGS.items():
        path = os.environ.get(meta["env_var"], "")
        pub_path = path + ".pub" if path else ""
        present = bool(path and os.path.isfile(path))
        pub_present = bool(pub_path and os.path.isfile(pub_path))
        comment = _read_pub_comment(pub_path) if pub_present else None
        items.append(KeyStatusItem(
            id=key_id,
            label=meta["label"],
            env_var=meta["env_var"],
            path=path or "(not configured)",
            present=present,
            pub_present=pub_present,
            comment=comment,
        ))
    return KeysStatusOut(keys=items)


@router.post("/import", response_model=KeysImportOut)
async def keys_import(body: KeysImportIn) -> KeysImportOut:
    """
    Write private + public key files for each entry in the request body.

    Only ids present in KEY_CONFIGS are accepted.  Paths are resolved from
    env vars — never from client-supplied values.
    """
    results: list[KeyImportResult] = []

    for item in body.keys:
        if item.id not in cfg.KEY_CONFIGS:
            results.append(KeyImportResult(
                id=item.id,
                status="skipped",
                detail="Unknown key id — not in KEY_CONFIGS",
            ))
            continue

        try:
            priv_path = _resolve_path(item.id)
        except HTTPException as exc:
            results.append(KeyImportResult(id=item.id, status="skipped", detail=exc.detail))
            continue

        pub_path = priv_path + ".pub"

        try:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(priv_path), exist_ok=True)

            # Write private key — mode 0o600
            _safe_write(priv_path, item.private, 0o600)

            # Write public key — mode 0o644
            _safe_write(pub_path, item.public, 0o644)

            log.info("keys/import: wrote key id=%s to %s", item.id, priv_path)
            results.append(KeyImportResult(id=item.id, status="written"))
        except Exception as exc:
            log.error("keys/import: failed to write key id=%s: %s", item.id, exc)
            results.append(KeyImportResult(id=item.id, status="failed", detail=str(exc)))

    return KeysImportOut(results=results)


@router.delete("/{key_id}", status_code=204)
async def keys_delete(key_id: str) -> None:
    """Delete private + public key files for the given key id."""
    priv_path = _resolve_path(key_id)
    pub_path = priv_path + ".pub"

    deleted_any = False
    for path in (priv_path, pub_path):
        if os.path.isfile(path):
            try:
                os.unlink(path)
                log.info("keys/delete: removed %s", path)
                deleted_any = True
            except Exception as exc:
                log.error("keys/delete: failed to remove %s: %s", path, exc)
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to delete {os.path.basename(path)}: {exc}",
                )

    if not deleted_any:
        raise HTTPException(status_code=404, detail=f"No key files found for id {key_id!r}")


# ── Encrypted key store ───────────────────────────────────────────────────────

KEYSTORE_PATH = os.environ.get("BLUEPRINTS_KEYSTORE_PATH", "/root/.blueprints-keystore.enc")


class KeystorePayload(BaseModel):
    blob: str


@router.get("/store")
async def keystore_get():
    """
    Return the encrypted key-store blob for client-side decryption.
    The blob is ciphertext produced by the browser — this endpoint never
    sees or logs the password or plaintext key material.
    """
    if not os.path.isfile(KEYSTORE_PATH):
        raise HTTPException(status_code=404, detail="No encrypted key store found on this node.")
    try:
        with open(KEYSTORE_PATH) as f:
            blob = f.read().strip()
        return {"blob": blob}
    except Exception as exc:
        log.error("keystore/get: failed to read store: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to read key store.")


@router.put("/store", status_code=204)
async def keystore_put(body: KeystorePayload) -> None:
    """
    Save an encrypted key-store blob.  The blob is opaque ciphertext from
    the browser — we only verify it is valid JSON and store it atomically.
    Content is NEVER logged.
    """
    try:
        json.loads(body.blob)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid blob: must be valid JSON.")
    try:
        tmp = KEYSTORE_PATH + ".tmp"
        with open(tmp, "w") as f:
            f.write(body.blob)
        os.chmod(tmp, 0o600)
        os.chown(tmp, 0, 0)
        os.replace(tmp, KEYSTORE_PATH)
        log.info("keystore/put: encrypted store updated at %s", KEYSTORE_PATH)
    except Exception as exc:
        log.error("keystore/put: failed to write store: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to write key store: {exc}")
