#!/usr/bin/env python3
"""import_bookmarks.py — Bulk import Edge/Chrome/Firefox Netscape bookmark HTML into Blueprints.

Usage:
    python3 import_bookmarks.py --file /path/to/favorites.html [--api http://127.0.0.1:8080] [--no-skip-duplicates]

The script parses the Netscape Bookmark HTML format exported by Edge, Chrome, and Firefox,
then POSTs to /api/v1/bookmarks/import in batches.  Loopback requests bypass API auth.

Arguments:
    --file                  Path to the exported HTML file (required)
    --api                   Blueprints API base URL (default: http://127.0.0.1:8080)
    --no-skip-duplicates    Import even if the URL already exists (default: skip duplicates)
    --batch-size            Number of bookmarks per POST request (default: 200)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from html.parser import HTMLParser

# ── Parser ─────────────────────────────────────────────────────────────────

class _BookmarkParser(HTMLParser):
    """Walk the Netscape bookmark HTML structure and collect bookmark records."""

    def __init__(self) -> None:
        super().__init__()
        self.bookmarks: list[dict] = []
        self._folder_stack: list[str] = []
        self._pending_folder: str | None = None
        self._in_h3 = False
        self._in_a  = False
        self._current_href: str | None = None
        self._buf = ""

    def handle_starttag(self, tag: str, attrs: list) -> None:
        tag = tag.upper()
        d = dict(attrs)
        if tag == "DL":
            self._folder_stack.append(self._pending_folder or "")
            self._pending_folder = None
        elif tag == "H3":
            self._in_h3 = True
            self._buf   = ""
        elif tag == "A":
            href = d.get("href", "")
            if href and not re.match(r"^(javascript:|about:)", href, re.I):
                self._in_a         = True
                self._current_href = href
                self._buf          = ""

    def handle_endtag(self, tag: str) -> None:
        tag = tag.upper()
        if tag == "DL":
            if self._folder_stack:
                self._folder_stack.pop()
        elif tag == "H3":
            self._pending_folder = self._buf.strip()
            self._in_h3 = False
        elif tag == "A" and self._in_a:
            url   = self._current_href or ""
            title = self._buf.strip() or url
            folder_parts = [f for f in self._folder_stack if f]
            folder = "/".join(folder_parts) or None
            tags   = [
                re.sub(r"[^a-z0-9]+", "-", f.lower()).strip("-")
                for f in folder_parts
            ]
            tags = [t for t in tags if t]
            self.bookmarks.append({
                "url":         url,
                "title":       title,
                "folder":      folder,
                "tags":        tags,
                "description": None,
                "notes":       None,
                "favicon_url": None,
                "source":      "import",
            })
            self._in_a         = False
            self._current_href = None

    def handle_data(self, data: str) -> None:
        if self._in_h3 or self._in_a:
            self._buf += data


def parse_file(html_path: str) -> list[dict]:
    with open(html_path, encoding="utf-8", errors="replace") as fh:
        content = fh.read()
    parser = _BookmarkParser()
    parser.feed(content)
    return parser.bookmarks


# ── Import ─────────────────────────────────────────────────────────────────

def import_bookmarks(
    bookmarks: list[dict],
    api_base: str,
    skip_duplicates: bool = True,
    batch_size: int = 200,
) -> tuple[int, int]:
    """POST bookmarks to /api/v1/bookmarks/import in batches.

    Returns (total_imported, total_skipped).
    """
    url    = f"{api_base.rstrip('/')}/api/v1/bookmarks/import"
    total_imported = 0
    total_skipped  = 0

    for start in range(0, len(bookmarks), batch_size):
        batch = bookmarks[start : start + batch_size]
        payload = json.dumps({
            "bookmarks":       batch,
            "skip_duplicates": skip_duplicates,
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                total_imported += result.get("imported", 0)
                total_skipped  += result.get("skipped_duplicates", 0)
            end = min(start + batch_size, len(bookmarks))
            print(f"  Batch {start + 1}–{end}: {result.get('imported', 0)} imported, "
                  f"{result.get('skipped_duplicates', 0)} skipped")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch {start}–{start+batch_size}: HTTP {exc.code} — {body[:200]}",
                  file=sys.stderr)

    return total_imported, total_skipped


# ── CLI ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Import Netscape bookmark HTML into Blueprints.")
    parser.add_argument("--file",  required=True, help="Path to exported HTML file")
    parser.add_argument("--api",   default="http://127.0.0.1:8080", help="Blueprints API base URL")
    parser.add_argument("--no-skip-duplicates", dest="skip_duplicates",
                        action="store_false", help="Import even if URL already exists")
    parser.add_argument("--batch-size", type=int, default=200, help="Records per POST request")
    args = parser.parse_args()

    print(f"Parsing {args.file}…")
    bookmarks = parse_file(args.file)
    if not bookmarks:
        print("No bookmarks found.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(bookmarks)} bookmarks.")

    print(f"Importing to {args.api} (skip_duplicates={args.skip_duplicates})…")
    imported, skipped = import_bookmarks(
        bookmarks, args.api, args.skip_duplicates, args.batch_size
    )
    print(f"\nDone — imported {imported}, skipped {skipped} duplicates.")


if __name__ == "__main__":
    main()
