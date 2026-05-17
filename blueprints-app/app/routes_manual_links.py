"""routes_manual_links.py — CRUD for /api/v1/manual-links

Manually curated links — VLAN and tailnet addresses for important services
that don't change often. Used as a fallback quick-reference rendered view
in the Blueprints GUI.

Each entry syncs automatically to all fleet peers via the standard gen-based
sync protocol.

POST /api/v1/manual-links/intake-url accepts desktop browser URL drops from the
Manual Links Interface. It checks exact URL duplicates before creating a link.
"""

import json
import uuid
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .ai_client import complete
from .db import get_conn, increment_gen
from .models import ManualLinkCreate, ManualLinkOut, ManualLinkUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/manual-links", tags=["manual-links"])


class ManualLinkUrlIntake(BaseModel):
    url: str
    category_id: str | None = None
    dry_run: bool = False


def _normalise_url(url: str) -> str:
    value = (url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(400, "url must be an http(s) URL")
    return value


def _label_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or parsed.netloc or url).removeprefix("www.")
    path = parsed.path.strip("/")
    if not path:
        return host
    tail = path.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ").strip()
    return tail[:60] or host


def _json_object_from_text(text: str) -> dict | None:
    value = (text or "").strip()
    if value.startswith("```"):
        lines = value.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        value = "\n".join(lines).strip()
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    start = value.find("{")
    end = value.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(value[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _category_path_map(conn) -> dict[str, str]:
    rows = conn.execute("SELECT category_id, label, parent_category_id FROM manual_link_categories").fetchall()
    by_id = {row["category_id"]: dict(row) for row in rows}
    paths: dict[str, str] = {}
    for row in rows:
        parts = []
        seen: set[str] = set()
        current = dict(row)
        while current and current["category_id"] not in seen:
            seen.add(current["category_id"])
            parts.insert(0, current["label"])
            parent_id = current.get("parent_category_id")
            current = by_id.get(parent_id) if parent_id else None
        paths[row["category_id"]] = " / ".join(parts)
    return paths


async def _plan_url_intake(url: str, target_category_id: str | None, categories: dict[str, str]) -> dict:
    fallback = {
        "label": _label_from_url(url),
        "category_id": target_category_id or "group:unsorted",
        "notes": "Added by Manual Links URL drop intake.",
        "ai_used": False,
        "ai_project": None,
        "ai_error": None,
    }
    prompt = {
        "url": url,
        "target_category_id": target_category_id,
        "categories": categories,
        "task": (
            "This URL is being added to a local Manual Links indexing system. "
            "Derive a rich, meaningful, concise display name from the full URL. "
            "Break apart subdomain, registered domain, path, query, and fragment; infer "
            "the endpoint purpose when the URL makes it clear. Include the service or "
            "domain name when it improves meaning. Avoid raw implementation tokens as "
            "the whole label when the URL implies a human concept. For webmail, mail, "
            "inbox, or app=mail URLs, prefer labels like '<Provider> Email Inbox'. "
            "Choose the best existing category_id. Prefer the target_category_id when "
            "it is plausible. Return JSON only with label, category_id, notes, and "
            "confidence."
        ),
    }
    messages = [
        {
            "role": "system",
            "content": (
                "You classify dragged browser URLs for a local Manual Links database. "
                "Return compact JSON only. Do not browse. Do not invent credentials or "
                "private details. Use only the URL and category list."
            ),
        },
        {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
    ]
    last_error = None
    for project_name in ("manual-links", "browser-links"):
        try:
            raw = await complete(
                project_name,
                messages,
                max_tokens=512,
                strip_think=True,
                no_think=False,
            )
            parsed = _json_object_from_text(raw)
            if not parsed:
                raise ValueError("AI response was not a JSON object")
            category_id = parsed.get("category_id") if parsed.get("category_id") in categories else fallback["category_id"]
            return {
                "label": str(parsed.get("label") or fallback["label"])[:120],
                "category_id": category_id,
                "notes": str(parsed.get("notes") or fallback["notes"])[:1000],
                "confidence": parsed.get("confidence"),
                "ai_used": True,
                "ai_project": project_name,
                "ai_error": None,
            }
        except Exception as exc:
            last_error = f"{project_name}: {exc}"
    return {**fallback, "ai_error": last_error}


def _ensure_unsorted_category(conn) -> tuple[str, bool]:
    category_id = "group:unsorted"
    row = conn.execute("SELECT category_id FROM manual_link_categories WHERE category_id=?", (category_id,)).fetchone()
    if row:
        return category_id, False
    conn.execute(
        """
        INSERT INTO manual_link_categories
            (category_id, label, icon, parent_category_id, sort_order, notes)
        VALUES (?, 'Unsorted', 'icons/hieroglyphs/eye-of-horus-blue.svg', NULL, 9999, 'Created for dragged browser links')
        """,
        (category_id,),
    )
    return category_id, True


def _row_to_out(row) -> ManualLinkOut:
    return ManualLinkOut(**dict(row))


@router.get("", response_model=list[ManualLinkOut])
async def list_manual_links() -> list[ManualLinkOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM manual_links ORDER BY sort_order, group_name, label"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=ManualLinkOut, status_code=201)
async def create_manual_link(body: ManualLinkCreate) -> ManualLinkOut:
    link_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO manual_links (
                link_id, vlan_ip, vlan_uri, tailnet_ip, tailnet_uri,
                label, icon, group_name, parent_id, sort_order,
                pve_host, is_internet, vm_id, vm_name, lxc_id, lxc_name, location, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                link_id,
                body.vlan_ip, body.vlan_uri, body.tailnet_ip, body.tailnet_uri,
                body.label, body.icon, body.group_name, body.parent_id,
                body.sort_order if body.sort_order is not None else 0,
                body.pve_host, body.is_internet if body.is_internet is not None else 0,
                body.vm_id, body.vm_name, body.lxc_id, body.lxc_name, body.location, body.notes,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "manual_links", link_id, dict(row), gen)
    return _row_to_out(row)


@router.post("/intake-url")
async def intake_manual_link_url(body: ManualLinkUrlIntake) -> dict:
    url = _normalise_url(body.url)
    with get_conn() as conn:
        category_paths = _category_path_map(conn)
        target_category_id = body.category_id if body.category_id in category_paths else None
        unsorted_created = False
        if not target_category_id:
            target_category_id, unsorted_created = _ensure_unsorted_category(conn)
            category_paths[target_category_id] = "Unsorted"
            if unsorted_created:
                gen = increment_gen(conn, "human")
                cat_row = conn.execute(
                    "SELECT * FROM manual_link_categories WHERE category_id=?",
                    (target_category_id,),
                ).fetchone()
                enqueue_for_all_peers(conn, "INSERT", "manual_link_categories", target_category_id, dict(cat_row), gen)

    plan = await _plan_url_intake(url, target_category_id, category_paths)
    category_id = plan.get("category_id") if plan.get("category_id") in category_paths else target_category_id

    if body.dry_run:
        return {
            "created": False,
            "dry_run": True,
            "link_id": None,
            "label": plan.get("label") or _label_from_url(url),
            "category_id": category_id,
            "category_label": category_paths.get(category_id, "Unsorted"),
            "url": url,
            "ai_used": bool(plan.get("ai_used")),
            "ai_project": plan.get("ai_project"),
            "ai_error": plan.get("ai_error"),
            "notes": plan.get("notes"),
        }

    with get_conn() as conn:
        existing = conn.execute(
            """
            SELECT * FROM manual_links
            WHERE vlan_uri=? OR tailnet_uri=? OR vlan_ip=? OR tailnet_ip=?
            LIMIT 1
            """,
            (url, url, url, url),
        ).fetchone()
        created = False
        updated_existing_label = False
        if existing:
            link_id = existing["link_id"]
            link_row = existing
            planned_label = (plan.get("label") or "").strip()
            existing_label = (existing["label"] or "").strip()
            fallback_label = _label_from_url(url)
            if plan.get("ai_used") and planned_label and planned_label != existing_label and existing_label in ("", fallback_label):
                conn.execute(
                    """
                    UPDATE manual_links
                    SET label=?, updated_at=datetime('now')
                    WHERE link_id=?
                    """,
                    (planned_label, link_id),
                )
                updated_existing_label = True
                link_row = conn.execute("SELECT * FROM manual_links WHERE link_id=?", (link_id,)).fetchone()
        else:
            link_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO manual_links (
                    link_id, vlan_ip, vlan_uri, tailnet_ip, tailnet_uri,
                    label, icon, group_name, parent_id, sort_order,
                    pve_host, is_internet, vm_id, vm_name, lxc_id, lxc_name, location, notes
                ) VALUES (?, NULL, ?, NULL, NULL, ?, ?, ?, NULL, ?, NULL, 1, NULL, NULL, NULL, NULL, NULL, ?)
                """,
                (
                    link_id,
                    url,
                    plan.get("label") or _label_from_url(url),
                    "icons/hieroglyphs/eye-of-horus-blue.svg",
                    category_paths.get(category_id, "Unsorted").split(" / ")[-1],
                    0,
                    plan.get("notes") or "Added by Manual Links URL drop intake.",
                ),
            )
            created = True
            link_row = conn.execute("SELECT * FROM manual_links WHERE link_id=?", (link_id,)).fetchone()
        mapping_id = f"{category_id}:{link_id}"
        mapping = conn.execute(
            "SELECT mapping_id FROM manual_link_category_items WHERE mapping_id=?",
            (mapping_id,),
        ).fetchone()
        if not mapping:
            conn.execute(
                """
                INSERT INTO manual_link_category_items
                    (mapping_id, category_id, link_id, sort_order, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (mapping_id, category_id, link_id, 0, "Added by Manual Links URL drop intake."),
            )
        gen = increment_gen(conn, "human")
        if created:
            enqueue_for_all_peers(conn, "INSERT", "manual_links", link_id, dict(link_row), gen)
        elif updated_existing_label:
            enqueue_for_all_peers(conn, "UPDATE", "manual_links", link_id, dict(link_row), gen)
        raw_mapping = conn.execute(
            "SELECT * FROM manual_link_category_items WHERE mapping_id=?",
            (mapping_id,),
        ).fetchone()
        if raw_mapping and not mapping:
            enqueue_for_all_peers(conn, "INSERT", "manual_link_category_items", mapping_id, dict(raw_mapping), gen)
        category_row = conn.execute(
            "SELECT label FROM manual_link_categories WHERE category_id=?",
            (category_id,),
        ).fetchone()
    return {
        "created": created,
        "dry_run": False,
        "updated_existing_label": updated_existing_label,
        "link_id": link_id,
        "label": link_row["label"],
        "category_id": category_id,
        "category_label": category_row["label"] if category_row else category_paths.get(category_id, "Unsorted"),
        "url": url,
        "ai_used": bool(plan.get("ai_used")),
        "ai_project": plan.get("ai_project"),
        "ai_error": plan.get("ai_error"),
    }


@router.get("/{link_id}", response_model=ManualLinkOut)
async def get_manual_link(link_id: str) -> ManualLinkOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"Link {link_id!r} not found")
    return _row_to_out(row)


@router.put("/{link_id}", response_model=ManualLinkOut)
async def update_manual_link(link_id: str, body: ManualLinkUpdate) -> ManualLinkOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT link_id FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Link {link_id!r} not found")
        conn.execute(
            """
            UPDATE manual_links SET
                vlan_ip      = COALESCE(?, vlan_ip),
                vlan_uri     = COALESCE(?, vlan_uri),
                tailnet_ip   = COALESCE(?, tailnet_ip),
                tailnet_uri  = COALESCE(?, tailnet_uri),
                label        = COALESCE(?, label),
                icon         = COALESCE(?, icon),
                group_name   = COALESCE(?, group_name),
                parent_id    = COALESCE(?, parent_id),
                sort_order   = COALESCE(?, sort_order),
                pve_host     = COALESCE(?, pve_host),
                is_internet  = COALESCE(?, is_internet),
                vm_id        = COALESCE(?, vm_id),
                vm_name      = COALESCE(?, vm_name),
                lxc_id       = COALESCE(?, lxc_id),
                lxc_name     = COALESCE(?, lxc_name),
                location     = COALESCE(?, location),
                notes        = COALESCE(?, notes),
                updated_at   = datetime('now')
            WHERE link_id = ?
            """,
            (
                body.vlan_ip, body.vlan_uri, body.tailnet_ip, body.tailnet_uri,
                body.label, body.icon, body.group_name, body.parent_id,
                body.sort_order, body.pve_host, body.is_internet,
                body.vm_id, body.vm_name, body.lxc_id, body.lxc_name,
                body.location, body.notes, link_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_links", link_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{link_id}", status_code=204)
async def delete_manual_link(link_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM manual_links WHERE link_id=?", (link_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Link {link_id!r} not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "manual_links", link_id, {}, gen)
