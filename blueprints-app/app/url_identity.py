"""URL identity helpers for bookmarks and visit history."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Query parameters that only describe a cache-busting/versioned fetch, not the
# logical page. Keep this list deliberately small: many SPA routes use query
# strings as their only page selector.
VOLATILE_QUERY_PARAMS = frozenset({"_fresh"})


def normalize_url_identity(url: str) -> str:
    """Return the URL identity used for bookmark/visit de-duplication.

    The query string is normally preserved because SPA pages often encode real
    page selection there. Known volatile parameters, such as Blueprints'
    ``_fresh`` cache/version marker, are removed so page iterations collapse
    into one history entry.
    """
    parsed = urlparse((url or "").strip())
    path = parsed.path.rstrip("/") or "/"
    query = parsed.query

    if query:
        query_pairs = parse_qsl(query, keep_blank_values=True)
        identity_pairs = [
            (key, value)
            for key, value in query_pairs
            if key not in VOLATILE_QUERY_PARAMS
        ]
        if len(identity_pairs) != len(query_pairs):
            query = urlencode(identity_pairs, doseq=True)

    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            parsed.params,
            query,
            "",
        )
    )
