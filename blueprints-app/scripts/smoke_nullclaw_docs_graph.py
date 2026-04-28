#!/usr/bin/env python3
"""Smoke-check deterministic TurboVec Docs graph expansion and strict evidence."""

from __future__ import annotations

import argparse
import sys
from typing import Any

import httpx


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def post_json(client: httpx.Client, path: str, body: dict[str, Any]) -> dict[str, Any]:
    response = client.post(path, json=body)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def get_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def check_turbovec_graph(base_url: str) -> None:
    with httpx.Client(base_url=base_url.rstrip("/"), timeout=30.0) as client:
        stats = get_json(client, "/stats")
        graph = stats.get("graph")
        require(isinstance(graph, dict), "TurboVec /stats missing graph block")
        require(int(graph.get("nodes") or 0) > 0, "graph has no nodes")
        require(int(graph.get("edges") or 0) > 0, "graph has no edges")
        require(int(graph.get("headings") or 0) > 0, "graph has no headings")

        scoped = post_json(
            client,
            "/graph/expand",
            {
                "seed_paths": ["turbovec/RAG-RELIABILITY-LIFECYCLE-GRAPH-MAPREDUCE.md"],
                "allowed_paths": ["turbovec/"],
                "max_hops": 1,
                "max_docs": 6,
                "include_plans": True,
                "include_research": True,
                "include_history": False,
                "include_unknown": True,
            },
        )
        accepted = scoped.get("expanded")
        rejected = scoped.get("rejected")
        require(isinstance(accepted, list), "graph expansion missing expanded list")
        require(isinstance(rejected, list), "graph expansion missing rejected list")
        require(accepted, "scoped graph expansion accepted no in-scope docs")
        require(
            all(str(item.get("expanded_path") or "").startswith("turbovec/") for item in accepted),
            "graph expansion escaped allowed_paths",
        )
        require(
            any(item.get("reason") == "path_out_of_scope" for item in rejected),
            "graph expansion did not report an out-of-scope rejection",
        )
        for item in accepted:
            require(item.get("seed_path"), "accepted graph item missing seed_path")
            require(item.get("edge_type"), "accepted graph item missing edge_type")
            require(isinstance(item.get("hop_count"), int), "accepted graph item missing hop_count")
            require(item.get("lifecycle"), "accepted graph item missing lifecycle")
            require(item.get("source_type"), "accepted graph item missing source_type")
            require(item.get("authority"), "accepted graph item missing authority")

        filtered = post_json(
            client,
            "/graph/expand",
            {
                "seed_paths": ["turbovec/RAG-RELIABILITY-LIFECYCLE-GRAPH-MAPREDUCE.md"],
                "max_hops": 1,
                "max_docs": 12,
                "include_plans": True,
                "include_research": True,
                "include_history": False,
                "include_unknown": True,
            },
        )
        filtered_rejected = filtered.get("rejected")
        require(isinstance(filtered_rejected, list), "filtered graph expansion missing rejected list")
        require(
            any(item.get("reason") == "lifecycle_out_of_scope" for item in filtered_rejected),
            "graph expansion did not report lifecycle filtering",
        )
    print("ok turbovec-docs graph sidecar expansion")


def check_nullclaw_strict_evidence(worker_url: str) -> None:
    body = {
        "query": "RAG reliability lifecycle graph map reduce direction",
        "mode": "prompt_context",
        "search_profile": "turbovec-docs-hybrid",
        "folder": "turbovec",
        "allowed_paths": ["turbovec/"],
        "max_docs": 3,
        "max_chars_per_doc": 2500,
        "top_k": 5,
        "rerank": True,
        "follow_markdown_links": False,
        "graph_expand": True,
        "max_graph_hops": 1,
        "max_graph_docs": 6,
        "include_plans": True,
        "include_research": True,
        "include_history": False,
        "include_unknown": True,
        "map_reduce": True,
    }
    with httpx.Client(base_url=worker_url.rstrip("/"), timeout=90.0) as client:
        task = post_json(client, "/tasks/query-synthesis", body)
    require(task.get("state") == "succeeded", f"query synthesis failed: {task.get('error')}")
    result = task.get("result")
    require(isinstance(result, dict), "task missing result")
    strict = result.get("strict_evidence")
    require(isinstance(strict, dict), "result missing strict_evidence")
    require(strict.get("answerable") is True, f"strict evidence not answerable: {strict.get('answerability')}")
    require(
        strict.get("answerability") in {"supported", "supported_with_unknown_metadata", "direction_only"},
        f"unexpected answerability: {strict.get('answerability')}",
    )
    graph = strict.get("graph_expansion")
    require(isinstance(graph, dict) and graph.get("enabled") is True, "strict evidence missing graph_expansion")
    require(graph.get("accepted"), "strict evidence graph_expansion accepted no docs")
    require(graph.get("rejected_count", 0) >= 1, "strict evidence did not preserve graph rejection count")

    sources = strict.get("sources")
    require(isinstance(sources, list), "strict evidence missing source packets")
    graph_sources = [
        source for source in sources
        if isinstance(source, dict) and source.get("retrieval_stage") == "graph_expansion"
    ]
    require(graph_sources, "strict evidence has no graph-expanded source")
    for source in graph_sources:
        require(source.get("seed_path"), "graph source missing seed_path")
        require(source.get("graph_edge_type"), "graph source missing edge type")
        require(source.get("graph_hop_count") == 1, "graph source has unexpected hop count")
        require(source.get("lifecycle"), "graph source missing lifecycle")
        require(source.get("source_type"), "graph source missing source_type")
        require(source.get("authority"), "graph source missing authority")

    map_reduce = strict.get("map_reduce")
    require(isinstance(map_reduce, dict) and map_reduce.get("enabled") is True, "map_reduce not enabled")
    counts = strict.get("claim_count_by_category")
    require(isinstance(counts, dict), "strict evidence missing source category counts")
    require(sum(int(value or 0) for value in counts.values()) == strict.get("claim_count"), "claim count mismatch")
    claims = strict.get("claims")
    require(isinstance(claims, dict), "strict evidence missing claims")
    flat_claims = [
        claim
        for claim_list in claims.values()
        if isinstance(claim_list, list)
        for claim in claim_list
        if isinstance(claim, dict)
    ]
    require(flat_claims, "map_reduce produced no claims")
    require(
        any(claim.get("retrieval_stage") == "graph_expansion" for claim in flat_claims),
        "map_reduce did not consume graph-expanded docs",
    )
    require(
        all(claim.get("source_category") in {"current", "planned", "background", "low_authority", "unknown"} for claim in flat_claims),
        "claim source_category outside strict set",
    )
    print(f"ok nullclaw-docs-search graph strict_evidence task_id={task.get('id')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--turbovec-url", default="http://127.0.0.1:19080")
    parser.add_argument("--worker-url", default="http://127.0.0.1:19081")
    args = parser.parse_args()

    check_turbovec_graph(args.turbovec_url)
    check_nullclaw_strict_evidence(args.worker_url)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
