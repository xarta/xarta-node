#!/usr/bin/env python3
"""Count tokens with the Qwen3.6 tokenizer, with a conservative fallback."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path

DEFAULT_MODEL = "Qwen/Qwen3.6-35B-A3B"


def _read_text(args: argparse.Namespace) -> str:
    if args.text is not None:
        return args.text
    if args.path == "-":
        return sys.stdin.read()
    return Path(args.path).read_text(encoding=args.encoding)


def _heuristic_count(text: str) -> int:
    # Conservative English/code-ish fallback for Qwen-family BPE:
    # chars/3.4 is usually high enough to avoid under-budgeting ordinary docs.
    return max(1, math.ceil(len(text) / 3.4))


def _load_tokenizer(model: str):
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    local_path = hf_hub_download(repo_id=model, filename="tokenizer.json")
    return Tokenizer.from_file(local_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default="-", help="UTF-8 text file, or - for stdin")
    parser.add_argument("--text", help="literal text to count instead of reading a file")
    parser.add_argument("--model", default=os.environ.get("QWEN_TOKENIZER_MODEL", DEFAULT_MODEL))
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args()

    text = _read_text(args)
    method = "qwen-tokenizer"
    warning = None
    try:
        tokenizer = _load_tokenizer(args.model)
        token_count = len(tokenizer.encode(text).ids)
    except Exception as exc:  # deterministic fallback beats failing blind
        method = "heuristic-chars-div-3.4"
        warning = f"{type(exc).__name__}: {exc}"
        token_count = _heuristic_count(text)

    payload = {
        "tokens": token_count,
        "chars": len(text),
        "words": len(text.split()),
        "model": args.model,
        "method": method,
        "warning": warning,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"tokens={token_count} chars={len(text)} words={len(text.split())} method={method}")
        if warning:
            print(f"warning={warning}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
