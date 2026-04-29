"""routes_pwa.py — public PWA manifest endpoint for per-node WebAPK identity.

This route is intentionally token-exempt so browser install flows can fetch the
manifest without an API key prompt.
"""

import hashlib
import re
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from . import config as cfg

router = APIRouter(prefix="/pwa", tags=["pwa"])


_ICON_WEBAPP_DIR = Path("/xarta-node/gui-fallback/assets/icons/webapp")

# Stable non-sensitive palette used when a node does not define pwa_theme_color.
_DEFAULT_THEME_PALETTE: tuple[str, ...] = (
    "#2f7fd7",
    "#2f9b5f",
    "#c54b4b",
    "#8b6fd6",
    "#c88a2d",
    "#2d62a8",
    "#3a7d7a",
    "#7059c2",
)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _default_theme_color(node: dict) -> str:
    key = str(node.get("node_id") or cfg.NODE_ID or "blueprints")
    idx = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:2], 16) % len(_DEFAULT_THEME_PALETTE)
    return _DEFAULT_THEME_PALETTE[idx]


def _self_node() -> dict:
    node = cfg.SELF_NODE or {}
    return node if isinstance(node, dict) else {}


def _default_name(node: dict) -> str:
    display = str(node.get("display_name") or cfg.NODE_NAME or cfg.NODE_ID).strip()
    return f"Blueprints {display}".strip()


def _default_short_name(node: dict) -> str:
    display = str(node.get("display_name") or cfg.NODE_ID or "Blueprints").strip()
    return f"BP {display}".strip()


def _matching_icon_paths(key: str) -> tuple[str, str] | None:
    p192 = _ICON_WEBAPP_DIR / f"{key}_x192.png"
    p512 = _ICON_WEBAPP_DIR / f"{key}_x512.png"
    if p192.is_file() and p512.is_file():
        return (
            f"/fallback-ui/assets/icons/webapp/{p192.name}",
            f"/fallback-ui/assets/icons/webapp/{p512.name}",
        )
    return None


def _default_icon_paths(node: dict) -> tuple[str, str]:
    node_id = str(node.get("node_id") or cfg.NODE_ID or "").strip()
    display = str(node.get("display_name") or "").strip()

    candidates = []
    for value in (
        node_id,
        node_id.replace("-", "_"),
        _slug(node_id),
        display,
        display.replace(" ", "_"),
        _slug(display),
        display.title().split(" ")[0] if display else "",
    ):
        if value and value not in candidates:
            candidates.append(value)

    for key in candidates:
        paths = _matching_icon_paths(key)
        if paths:
            return paths

    # Final fallback: pick the first complete x192/x512 pair present.
    if _ICON_WEBAPP_DIR.is_dir():
        for p192 in sorted(_ICON_WEBAPP_DIR.glob("*_x192.png")):
            key = p192.name[:-9]
            paths = _matching_icon_paths(key)
            if paths:
                return paths

    return (
        "/fallback-ui/assets/icons/fallback.svg",
        "/fallback-ui/assets/icons/fallback.svg",
    )


def _manifest_icon(src: str, size: str) -> dict:
    if src.lower().endswith(".svg"):
        return {
            "src": src,
            "sizes": "any",
            "type": "image/svg+xml",
            "purpose": "any maskable",
        }
    return {
        "src": src,
        "sizes": size,
        "type": "image/png",
        "purpose": "any maskable",
    }


@router.get("/manifest", include_in_schema=False)
async def pwa_manifest() -> JSONResponse:
    node = _self_node()
    node_id = str(node.get("node_id") or cfg.NODE_ID).strip()

    name = str(node.get("pwa_name") or _default_name(node)).strip()
    short_name = str(node.get("pwa_short_name") or _default_short_name(node)).strip()

    icon_192_default, icon_512_default = _default_icon_paths(node)
    icon_192 = str(node.get("pwa_icon_192") or icon_192_default).strip()
    icon_512 = str(node.get("pwa_icon_512") or icon_512_default).strip()

    theme_color = str(node.get("pwa_theme_color") or _default_theme_color(node)).strip()
    background_color = str(node.get("pwa_background_color") or "#0f1117").strip()

    manifest = {
        "id": f"/fallback-ui/?source=pwa&node={node_id}",
        "name": name,
        "short_name": short_name,
        "description": f"Blueprints dashboard for {node.get('display_name') or node_id}",
        "start_url": f"/fallback-ui/?source=pwa&node={node_id}",
        "scope": "/fallback-ui/",
        "display": "fullscreen",
        "display_override": ["fullscreen", "standalone", "minimal-ui"],
        "background_color": background_color,
        "theme_color": theme_color,
        "orientation": "any",
        "icons": [_manifest_icon(icon_192, "192x192"), _manifest_icon(icon_512, "512x512")],
        "shortcuts": [
            {
                "name": "Synthesis",
                "short_name": "Synthesis",
                "url": "/fallback-ui/?tab=synthesis",
            },
            {
                "name": "Probes",
                "short_name": "Probes",
                "url": "/fallback-ui/?tab=probes",
            },
            {
                "name": "Settings",
                "short_name": "Settings",
                "url": "/fallback-ui/?tab=settings",
            },
        ],
    }

    return JSONResponse(content=manifest, media_type="application/manifest+json")
