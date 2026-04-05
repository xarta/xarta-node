"""routes_pwa.py — public PWA manifest endpoint for per-node WebAPK identity.

This route is intentionally token-exempt so browser install flows can fetch the
manifest without an API key prompt.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from . import config as cfg

router = APIRouter(prefix="/pwa", tags=["pwa"])


_NODE_THEME_COLORS: dict[str, str] = {
    # Thunderbird craft-inspired palette
    "thunderbird-1": "#2f7fd7",
    "thunderbird-2": "#2f9b5f",
    "thunderbird-3": "#c54b4b",
    # Tracy uniforms: blue family
    "scott-tracy": "#2d62a8",
    "virgil-tracy": "#2d62a8",
    "alan-tracy": "#2d62a8",
}

_NODE_ICON_KEYS: dict[str, str] = {
    "thunderbird-1": "thunderbird-1",
    "thunderbird-2": "thunderbird-2",
    "thunderbird-3": "thunderbird-3",
    "scott-tracy": "Scott",
    "virgil-tracy": "Virgil",
    "alan-tracy": "Alan",
}


def _self_node() -> dict:
    node = cfg.SELF_NODE or {}
    return node if isinstance(node, dict) else {}


def _default_name(node: dict) -> str:
    display = str(node.get("display_name") or cfg.NODE_NAME or cfg.NODE_ID).strip()
    return f"Blueprints {display}".strip()


def _default_short_name(node: dict) -> str:
    display = str(node.get("display_name") or cfg.NODE_ID or "Blueprints").strip()
    return f"BP {display}".strip()


def _default_icon_paths(node: dict) -> tuple[str, str]:
    node_id = str(node.get("node_id") or cfg.NODE_ID or "").strip()
    key = _NODE_ICON_KEYS.get(node_id, node_id or "thunderbird-1")
    return (
        f"/fallback-ui/assets/icons/webapp/{key}_x192.png",
        f"/fallback-ui/assets/icons/webapp/{key}_x512.png",
    )


@router.get("/manifest", include_in_schema=False)
async def pwa_manifest() -> JSONResponse:
    node = _self_node()
    node_id = str(node.get("node_id") or cfg.NODE_ID).strip()

    name = str(node.get("pwa_name") or _default_name(node)).strip()
    short_name = str(node.get("pwa_short_name") or _default_short_name(node)).strip()

    icon_192_default, icon_512_default = _default_icon_paths(node)
    icon_192 = str(node.get("pwa_icon_192") or icon_192_default).strip()
    icon_512 = str(node.get("pwa_icon_512") or icon_512_default).strip()

    theme_color = str(node.get("pwa_theme_color") or _NODE_THEME_COLORS.get(node_id, "#2f7fd7")).strip()
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
        "icons": [
            {
                "src": icon_192,
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any maskable",
            },
            {
                "src": icon_512,
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any maskable",
            },
        ],
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