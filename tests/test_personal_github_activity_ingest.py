import importlib.util
import re
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "blueprints-app"
    / "scripts"
    / "personal_github_activity_ingest.py"
)
APP_DB_PATH = Path(__file__).resolve().parents[1] / "blueprints-app" / "app" / "db.py"
ROUTES_SYNC_PATH = Path(__file__).resolve().parents[1] / "blueprints-app" / "app" / "routes_sync.py"
GIT_TABLE_PRIMARY_KEYS = {
    "personal_git_repositories": "repo_full_name",
    "personal_git_commits": "commit_id",
    "personal_git_features": "feature_id",
    "personal_git_kanban_arcs": "arc_id",
    "personal_git_daily_summaries": "summary_id",
    "personal_git_import_runs": "run_id",
}
spec = importlib.util.spec_from_file_location("personal_github_activity_ingest", SCRIPT_PATH)
ingest = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = ingest
spec.loader.exec_module(ingest)
TEST_LLM_MODEL = "TEST-LOCAL-LLM-MODEL"
ingest.DEFAULT_LLM_MODEL = TEST_LLM_MODEL


@pytest.fixture(autouse=True)
def _disable_live_runtime_guard(monkeypatch):
    monkeypatch.setattr(ingest, "require_runtime_readiness_for_live_apply", lambda *_args: None)


def _repo(full_name="davros1973/xarta-node", *, owner="davros1973", can_push=True):
    return ingest.RepoRecord(
        full_name=full_name,
        repo_id=42,
        owner=owner,
        name=full_name.split("/", 1)[1],
        html_url=f"https://github.com/{full_name}",
        description="Blueprints test repo",
        default_branch="main",
        visibility="private",
        is_private=True,
        is_fork=False,
        is_archived=False,
        can_push=can_push,
        last_pushed_at="2026-06-22T10:00:00Z",
        raw={"permissions": {"push": can_push}},
    )


def _commit(repo, sha, subject, *, local_date="2026-06-22", feature_key=None):
    key = feature_key or ingest.infer_feature_key(repo, subject, ["main"])
    return ingest.CommitRecord(
        commit_id=f"ghc-{ingest.stable_digest(repo.full_name + ':' + sha, 24)}",
        repo_full_name=repo.full_name,
        sha=sha,
        short_sha=sha[:7],
        html_url=f"https://github.com/{repo.full_name}/commit/{sha}",
        author_login="davros1973",
        author_name="Davros",
        committed_at=f"{local_date}T12:00:00Z",
        local_date=local_date,
        message_subject=subject,
        message_body="",
        branches=["main"],
        pr_refs=[],
        issue_refs=[],
        feature_key=key,
        source_hash=ingest.stable_digest(
            {"repo": repo.full_name, "sha": sha, "subject": subject}, 32
        ),
        provenance={"repo": repo.full_name, "sha": sha, "branches": ["main"]},
    )


def test_git_projection_schema_matches_app_schema_and_sync_registration():
    app_schema = APP_DB_PATH.read_text(encoding="utf-8")
    sync_source = ROUTES_SYNC_PATH.read_text(encoding="utf-8")

    for table, primary_key in GIT_TABLE_PRIMARY_KEYS.items():
        assert f"CREATE TABLE IF NOT EXISTS {table}" in ingest.GIT_SCHEMA_SQL
        assert f"CREATE TABLE IF NOT EXISTS {table}" in app_schema
        assert re.search(rf"\b{re.escape(primary_key)}\s+[^,\n]*PRIMARY KEY", app_schema)
        assert f'"{table}"' in sync_source
        assert f'"{table}": "{primary_key}"' in sync_source


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE personal_sources (
            source_id TEXT PRIMARY KEY,
            source_type TEXT,
            label TEXT,
            status TEXT,
            last_seen_at TEXT,
            health_json TEXT,
            provenance_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE personal_events (
            event_id TEXT PRIMARY KEY,
            source_type TEXT,
            source_ref TEXT,
            source_hash TEXT,
            kind TEXT,
            title TEXT,
            body_excerpt TEXT,
            content_projection TEXT,
            start_at TEXT,
            end_at TEXT,
            local_date TEXT,
            timezone TEXT,
            status TEXT,
            priority TEXT,
            privacy_level TEXT,
            tags_json TEXT,
            related_kanban_items_json TEXT,
            related_tasks_json TEXT,
            related_import_batches_json TEXT,
            file_refs_json TEXT,
            db_refs_json TEXT,
            provenance_json TEXT,
            projection_state TEXT,
            provenance_state TEXT,
            last_rendered_at TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE kanban_items (
            item_id TEXT PRIMARY KEY,
            parent_item_id TEXT,
            title TEXT,
            body_excerpt TEXT,
            item_type TEXT,
            state_id TEXT,
            priority_id TEXT,
            depth INTEGER,
            sort_order INTEGER,
            status TEXT,
            archived_at TEXT,
            promoted_from_ref TEXT,
            source_type TEXT,
            source_ref TEXT,
            source_hash TEXT,
            tags_json TEXT,
            related_event_ids_json TEXT,
            related_task_ids_json TEXT,
            related_issue_ids_json TEXT,
            search_text TEXT,
            search_metadata_json TEXT,
            embedding_ref TEXT,
            embedding_model TEXT,
            embedding_updated_at TEXT,
            vector_index_key TEXT,
            provenance_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE nodes (node_id TEXT PRIMARY KEY);
        CREATE TABLE sync_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_node_id TEXT,
            action_type TEXT,
            table_name TEXT,
            row_id TEXT,
            row_data TEXT,
            gen INTEGER,
            guid TEXT,
            sent INTEGER DEFAULT 0
        );
        INSERT INTO sync_meta(key, value) VALUES ('gen', '0');
        """
    )
    ingest.ensure_git_tables(conn)
    return conn


def _fixture_records():
    repo = _repo()
    commits = [
        _commit(repo, "a" * 40, "Add Calendar git filter", feature_key="blueprints-ui"),
        _commit(repo, "b" * 40, "Fix GitHub ingestion idempotency", feature_key="github-ingestion"),
    ]
    features = ingest.build_features(commits)
    summaries = ingest.build_daily_summaries(commits, {repo.full_name: repo}, features)
    return [repo], commits, features, summaries


def _fixture_report(*, include_commit_details=True):
    repos, commits, features, summaries = _fixture_records()
    return ingest.build_report(
        mode="dry-run",
        db_path=Path("/tmp/blueprints.db"),
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        tz_name="Etc/UTC",
        existing_git_summary_day=None,
        start_day=date(2026, 6, 22),
        end_day=date(2026, 6, 22),
        owners=["davros1973", "xarta"],
        writable_only=True,
        authors=["davros1973"],
        all_commit_authors=False,
        max_branches=0,
        repo_scope={"visible_total": 1, "allowed_total": 1},
        repos=repos,
        repo_fetch_reports={repos[0].full_name: {"branches": {"truncated": False}, "errors": []}},
        commits=commits,
        features=features,
        summaries=summaries,
        rate_limit_before={},
        include_commit_details=include_commit_details,
    )


def _llm_enriched_report():
    report = _fixture_report(include_commit_details=True)
    commit_ids = [commit["commit_id"] for commit in report["commits"]]
    report["llm_enrichment"] = {
        "source": "local_llm",
        "model": ingest.DEFAULT_LLM_MODEL,
        "endpoint": ingest.DEFAULT_LLM_ENDPOINT,
        "started_at": "2026-06-23T01:00:00Z",
        "day_count": 1,
        "arc_pass": {"chunk_count": 1},
        "arc_context": [
            {
                "project_key": "blueprints-personal",
                "project_title": "Blueprints Personal",
                "subproject_key": "git-activity",
                "subproject_title": "Git Activity",
                "feature_key": "calendar-git-summary",
                "feature_title": "Calendar Git Summary",
                "status": "active",
                "notes": "LLM-inferred feature arc.",
                "evidence_commit_ids": commit_ids,
            }
        ],
    }
    report["calendar_summaries"][0]["summary_source"] = "local_llm"
    report["calendar_summaries"][0]["title"] = "Git activity summary - 2026-06-22"
    report["calendar_summaries"][0]["markdown"] = (
        "# Git Activity - 2026-06-22\n\n"
        "GitHub history was folded into a readable Calendar summary and a "
        "[Calendar Git Summary](blueprints://kanban/items/work-git-feature-placeholder) "
        "Kanban thread without per-commit Calendar noise."
    )
    return ingest.attach_llm_record_preview(report)


def test_report_preserves_repo_scope_flags_for_cached_preflight():
    report = _fixture_report(include_commit_details=True)
    repo = report["repos_scanned"][0]

    assert repo["repo_id"] == 42
    assert repo["full_name"] == "davros1973/xarta-node"
    assert repo["owner"] == "davros1973"
    assert repo["name"] == "xarta-node"
    assert repo["visibility"] == "private"
    assert repo["is_private"] is True
    assert repo["is_fork"] is False
    assert repo["is_archived"] is False
    assert repo["can_push"] is True
    assert repo["default_branch"] == "main"
    assert repo["last_pushed_at"] == "2026-06-22T10:00:00Z"
    assert repo["html_url"] == "https://github.com/davros1973/xarta-node"
    assert repo["url"] == "https://github.com/davros1973/xarta-node"


def test_preflight_report_normalizes_legacy_cached_repo_scope_flags():
    source_report = _llm_enriched_report()
    for key in [
        "repo_id",
        "owner",
        "name",
        "is_private",
        "is_fork",
        "is_archived",
        "default_branch",
        "last_pushed_at",
        "html_url",
    ]:
        source_report["repos_scanned"][0].pop(key, None)
    records = ingest.records_from_enriched_report(source_report, require_llm=True)
    report = ingest.report_for_record_set(
        source_report,
        records,
        db_path=Path("/tmp/blueprints.db"),
        mode="apply-preflight",
        run_id="git-import-preflight-test",
        started_at="2026-06-23T00:00:00Z",
    )
    repo = report["repos_scanned"][0]

    assert repo["owner"] == "davros1973"
    assert repo["name"] == "xarta-node"
    assert repo["is_private"] is True
    assert repo["is_fork"] is False
    assert repo["is_archived"] is False
    assert repo["default_branch"] == ""
    assert repo["last_pushed_at"] == ""
    assert repo["html_url"] == "https://github.com/davros1973/xarta-node"


def _write_approved_preflight_report(source_report, source_report_path, preflight_path):
    source_report["report_path"] = str(source_report_path)
    records = ingest.records_from_enriched_report(source_report, require_llm=True)
    preflight = ingest.report_for_record_set(
        source_report,
        records,
        db_path=Path("/tmp/blueprints.db"),
        mode="apply-preflight",
        run_id="git-import-preflight-test",
        started_at="2026-06-23T00:00:00Z",
    )
    preflight_path.write_text(ingest.json.dumps(preflight), encoding="utf-8")
    return preflight


def _preflight_digest(preflight):
    return preflight["approval"]["approval_digest"]


def test_repo_scope_includes_only_writable_davros_or_xarta_repos():
    allowed = {"davros1973", "xarta"}
    assert ingest.repo_is_allowed(_repo(), allowed)
    assert ingest.repo_is_allowed(_repo("xarta/blueprints", owner="xarta"), allowed)
    assert not ingest.repo_is_allowed(
        _repo("All-About-AI-YouTube/one", owner="All-About-AI-YouTube", can_push=False),
        allowed,
    )
    assert not ingest.repo_is_allowed(_repo(can_push=False), allowed)


def test_last_summarized_git_day_uses_git_tag_or_source_type():
    conn = _conn()
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, tags_json, related_kanban_items_json,
            related_tasks_json, related_import_batches_json, file_refs_json, db_refs_json,
            provenance_json, projection_state, provenance_state
        )
        VALUES ('manual-1', 'manual-calendar', 'event', 'Manual', '2026-06-22', '["git"]',
                '[]', '[]', '[]', '[]', '[]', '{}', 'hot', 'linked')
        """
    )
    assert ingest.last_summarized_git_day(conn) == date(2026, 6, 22)


def test_apply_requires_explicit_bootstrap_without_existing_git_summary():
    conn = _conn()
    with pytest.raises(SystemExit):
        ingest.determine_window(
            conn,
            since_date=None,
            until_date=date(2026, 6, 22),
            bootstrap_days=7,
            apply=True,
            tz=ingest.ZoneInfo("Etc/UTC"),
        )


def test_cli_scanner_apply_requires_explicit_legacy_flag_before_db_open(tmp_path, monkeypatch):
    report_path = tmp_path / "scanner-apply.json"
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires --allow-scanner-apply"):
        ingest.main(
            [
                "--apply",
                "--since-date",
                "2026-06-22",
                "--until-date",
                "2026-06-22",
                "--report",
                str(report_path),
                "--quiet",
            ]
        )


def test_window_without_since_date_resumes_day_after_last_git_summary():
    conn = _conn()
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, tags_json, related_kanban_items_json,
            related_tasks_json, related_import_batches_json, file_refs_json, db_refs_json,
            provenance_json, projection_state, provenance_state
        )
        VALUES ('git-summary-2026-06-20', 'git', 'git-summary', 'Git summary', '2026-06-20', '["git"]',
                '[]', '[]', '[]', '[]', '[]', '{}', 'hot', 'linked')
        """
    )
    start_day, end_day, existing_day = ingest.determine_window(
        conn,
        since_date=None,
        until_date=date(2026, 6, 22),
        bootstrap_days=7,
        apply=False,
        tz=ingest.ZoneInfo("Etc/UTC"),
    )
    assert existing_day == date(2026, 6, 20)
    assert start_day == date(2026, 6, 21)
    assert end_day == date(2026, 6, 22)


def test_cli_dry_run_report_resumes_after_existing_git_summary(tmp_path, monkeypatch):
    conn = _conn()
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, tags_json,
            related_kanban_items_json, related_tasks_json, related_import_batches_json,
            file_refs_json, db_refs_json, provenance_json, projection_state, provenance_state
        )
        VALUES ('git-summary-2026-06-20', 'git', 'git-summary', 'Git summary', '2026-06-20',
                'Etc/UTC', '["git"]', '[]', '[]', '[]', '[]', '[]', '{}', 'hot', 'linked')
        """
    )
    report_path = tmp_path / "resume-report.json"
    monkeypatch.setattr(ingest, "connect_db", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(ingest, "fetch_rate_limit", lambda: {})
    monkeypatch.setattr(
        ingest, "enumerate_repositories", lambda **_kwargs: ([], {"visible_total": 0})
    )
    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )

    assert (
        ingest.main(
            [
                "--dry-run",
                "--until-date",
                "2026-06-22",
                "--report",
                str(report_path),
                "--quiet",
            ]
        )
        == 0
    )
    report = ingest.load_json_report(report_path)
    assert report["window"]["existing_git_summary_day"] == "2026-06-20"
    assert report["window"]["date_start"] == "2026-06-21"
    assert report["window"]["date_end"] == "2026-06-22"


def test_cli_dry_run_with_no_new_complete_days_skips_github(tmp_path, monkeypatch):
    conn = _conn()
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, tags_json,
            related_kanban_items_json, related_tasks_json, related_import_batches_json,
            file_refs_json, db_refs_json, provenance_json, projection_state, provenance_state
        )
        VALUES ('git-summary-2026-06-22', 'git', 'git-summary', 'Git summary', '2026-06-22',
                'Etc/UTC', '["git"]', '[]', '[]', '[]', '[]', '[]', '{}', 'hot', 'linked')
        """
    )
    report_path = tmp_path / "empty-resume-report.json"
    review_path = tmp_path / "empty-resume-report.md"
    monkeypatch.setattr(ingest, "connect_db", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(
        ingest, "fetch_rate_limit", lambda: pytest.fail("GitHub rate limit was checked")
    )
    monkeypatch.setattr(
        ingest,
        "enumerate_repositories",
        lambda **_kwargs: pytest.fail("GitHub repositories were enumerated"),
    )
    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )

    assert (
        ingest.main(
            [
                "--dry-run",
                "--until-date",
                "2026-06-22",
                "--report",
                str(report_path),
                "--review-report",
                str(review_path),
                "--quiet",
            ]
        )
        == 0
    )
    report = ingest.load_json_report(report_path)
    assert report["window"]["existing_git_summary_day"] == "2026-06-22"
    assert report["window"]["date_start"] == "2026-06-23"
    assert report["window"]["date_end"] == "2026-06-22"
    assert report["window"]["complete_day_count"] == 0
    assert report["message"] == "No complete unsummarized days to process."
    review = review_path.read_text(encoding="utf-8")
    assert "Message: No complete unsummarized days to process." in review


def test_requested_today_window_is_always_empty():
    conn = _conn()
    tz = ingest.ZoneInfo("Etc/UTC")
    today = ingest.datetime.now(tz).date()
    start_day, end_day, _ = ingest.determine_window(
        conn,
        since_date=today,
        until_date=today,
        bootstrap_days=7,
        apply=False,
        tz=tz,
    )
    assert start_day == today
    assert end_day == today - ingest.timedelta(days=1)
    assert ingest.date_range(start_day, end_day) == []


def test_zero_branch_limit_scans_all_current_branches(monkeypatch):
    repo = _repo()

    def fake_branch_api(endpoint):
        assert endpoint.endswith("/branches?per_page=100")
        return [{"name": "main"}, {"name": "feature/one"}, {"name": "feature/two"}]

    monkeypatch.setattr(ingest, "gh_api_base64_items", fake_branch_api)
    branches, report = ingest.fetch_branches(repo, 0)
    assert branches == ["main", "feature/one", "feature/two"]
    assert report["truncated"] is False
    assert report["limit"] == 0


def test_daily_markdown_is_aggregate_readable_and_hides_full_hashes():
    repos, commits, features, summaries = _fixture_records()
    assert len(summaries) == 1
    markdown = summaries[0].markdown
    assert "Git work touched 1 repository across 2 commits." in markdown
    assert "Feature Threads" in markdown
    assert ingest.kanban_item_url(next(iter(features.values())).related_kanban_item_id) in markdown
    assert "a" * 40 not in markdown
    assert "b" * 40 not in markdown
    assert markdown.count("- [davros1973/xarta-node]") == 1
    assert len(summaries) < len(commits)


def test_long_history_report_window_is_compact():
    repos, commits, features, summaries = _fixture_records()
    report = ingest.build_report(
        mode="dry-run",
        db_path=Path("/tmp/blueprints.db"),
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        tz_name="Etc/UTC",
        existing_git_summary_day=None,
        start_day=date(1970, 1, 1),
        end_day=date(2026, 6, 22),
        owners=["davros1973", "xarta"],
        writable_only=True,
        authors=["davros1973"],
        all_commit_authors=False,
        max_branches=0,
        repo_scope={"visible_total": 1, "allowed_total": 1},
        repos=repos,
        repo_fetch_reports={repos[0].full_name: {"branches": {"truncated": False}, "errors": []}},
        commits=commits,
        features=features,
        summaries=summaries,
        rate_limit_before={},
        include_commit_details=False,
    )
    assert "complete_days" not in report["window"]
    assert report["window"]["complete_day_count"] > 20000
    assert report["window"]["activity_day_count"] == 1
    assert report["window"]["activity_days"] == ["2026-06-22"]
    assert report["window"]["today_exclusion_policy"]["never_include_today"] is True


def test_review_markdown_is_compact_and_no_write_oriented():
    repos, commits, features, summaries = _fixture_records()
    report = ingest.build_report(
        mode="dry-run",
        db_path=Path("/tmp/blueprints.db"),
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        tz_name="Etc/UTC",
        existing_git_summary_day=None,
        start_day=date(1970, 1, 1),
        end_day=date(2026, 6, 22),
        owners=["davros1973", "xarta"],
        writable_only=True,
        authors=["davros1973"],
        all_commit_authors=False,
        max_branches=0,
        repo_scope={"visible_total": 1, "allowed_total": 1},
        repos=repos,
        repo_fetch_reports={repos[0].full_name: {"branches": {"truncated": False}, "errors": []}},
        commits=commits,
        features=features,
        summaries=summaries,
        rate_limit_before={},
        include_commit_details=True,
    )
    report["report_path"] = "/tmp/personal-github-activity-dry-run.json"
    markdown = ingest.build_review_markdown(report)
    assert "Dry-run only. No Blueprints database writes" in markdown
    assert "JSON report: `/tmp/personal-github-activity-dry-run.json`" in markdown
    assert "latest permitted date" in markdown
    assert "Scoped writable repos scanned: 1" in markdown
    assert "Commits dated today: 0" in markdown
    assert "## Latest Prepared Daily Summaries" in markdown
    assert "a" * 40 not in markdown
    assert "b" * 40 not in markdown
    assert ingest.visible_calendar_identifier_hits(markdown) == []


def test_review_from_existing_report_does_not_touch_github_or_db(tmp_path, monkeypatch):
    repos, commits, features, summaries = _fixture_records()
    report = ingest.build_report(
        mode="dry-run",
        db_path=Path("/tmp/blueprints.db"),
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        tz_name="Etc/UTC",
        existing_git_summary_day=None,
        start_day=date(1970, 1, 1),
        end_day=date(2026, 6, 22),
        owners=["davros1973", "xarta"],
        writable_only=True,
        authors=["davros1973"],
        all_commit_authors=False,
        max_branches=0,
        repo_scope={"visible_total": 1, "allowed_total": 1},
        repos=repos,
        repo_fetch_reports={repos[0].full_name: {"branches": {"truncated": False}, "errors": []}},
        commits=commits,
        features=features,
        summaries=summaries,
        rate_limit_before={},
        include_commit_details=False,
    )
    json_path = tmp_path / "report.json"
    review_path = tmp_path / "review.md"
    json_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )

    assert (
        ingest.main(
            [
                "--review-from-report",
                str(json_path),
                "--review-report",
                str(review_path),
                "--quiet",
            ]
        )
        == 0
    )
    markdown = review_path.read_text(encoding="utf-8")
    assert "Dry-run only" in markdown
    assert f"JSON report: `{json_path}`" in markdown


def test_apply_preflight_review_includes_acceptance_checks():
    source_report = _llm_enriched_report()
    records = ingest.records_from_enriched_report(source_report, require_llm=True)
    report = ingest.report_for_record_set(
        source_report,
        records,
        db_path=Path("/tmp/blueprints.db"),
        mode="apply-preflight",
        run_id="git-import-preflight-test",
        started_at="2026-06-23T00:00:00Z",
    )

    acceptance = report["acceptance_checks"]
    assert acceptance["all_passed"] is True
    assert [check["check_id"] for check in acceptance["checks"]] == [
        "no_today_activity",
        "local_llm_route",
        "calendar_day_coverage",
        "calendar_markdown",
        "calendar_markdown_readability",
        "calendar_kanban_links",
        "repository_scope",
        "no_database_writes",
    ]
    assert all(check["status"] == "pass" for check in acceptance["checks"])
    assert acceptance["checks"][3]["metrics"]["blank_markdown_count"] == 0
    assert acceptance["checks"][4]["metrics"]["visible_long_identifier_count"] == 0
    assert acceptance["checks"][5]["metrics"]["missing_kanban_link_count"] == 0

    markdown = ingest.build_review_markdown(report)
    approval = report["approval"]
    assert "## Approval Gate" in markdown
    assert f"Approval type: `{approval['approval_type']}`" in markdown
    assert f"Approval digest: `{approval['approval_digest']}`" in markdown
    assert f"Source report: `{approval['source_report_path']}`" in markdown
    assert "## Preflight Acceptance Checks" in markdown
    assert "No today activity: PASS" in markdown
    assert "Local LLM route: PASS" in markdown
    assert "Calendar day coverage: PASS" in markdown
    assert "Calendar Markdown: PASS (0 blank summaries)" in markdown
    assert "Calendar Markdown readability: PASS (0 visible long identifiers)" in markdown
    assert "Calendar Kanban links: PASS (0 summaries missing Kanban links)" in markdown
    assert "Repository scope: PASS" in markdown
    assert "No database writes: PASS" in markdown
    for summary in report["calendar_summaries"]:
        assert ingest.visible_calendar_identifier_hits(summary["markdown"]) == []


def test_preflight_acceptance_fails_visible_long_calendar_identifiers():
    report = _llm_enriched_report()
    report["mode"] = "apply-preflight"
    report["calendar_summaries"][0]["markdown"] = (
        "# Git Activity\n\n"
        "This prose accidentally exposed abcdef1234567890abcdef1234567890abcdef12."
    )

    acceptance = ingest.build_preflight_acceptance_checks(report)
    by_id = {check["check_id"]: check for check in acceptance["checks"]}
    assert acceptance["all_passed"] is False
    assert by_id["calendar_markdown"]["status"] == "pass"
    assert by_id["calendar_markdown_readability"]["status"] == "fail"
    assert by_id["calendar_markdown_readability"]["metrics"]["visible_long_identifier_count"] == 1


def test_preflight_acceptance_fails_missing_calendar_kanban_links():
    report = _llm_enriched_report()
    report["mode"] = "apply-preflight"
    report["calendar_summaries"][0]["markdown"] = "# Git Activity\n\nReadable but unlinked."

    acceptance = ingest.build_preflight_acceptance_checks(report)
    by_id = {check["check_id"]: check for check in acceptance["checks"]}
    assert acceptance["all_passed"] is False
    assert by_id["calendar_kanban_links"]["status"] == "fail"
    assert by_id["calendar_kanban_links"]["metrics"]["missing_kanban_link_count"] == 1


def test_llm_default_model_comes_from_configuration():
    assert ingest.DEFAULT_LLM_MODEL == TEST_LLM_MODEL
    parser = ingest.build_parser()
    args = parser.parse_args(["--llm-enrich-from-report", "/tmp/report.json"])
    assert args.llm_model == ingest.DEFAULT_LLM_MODEL


def test_llm_enrichment_requires_configured_model(monkeypatch):
    monkeypatch.setattr(ingest, "DEFAULT_LLM_MODEL", "")
    parser = ingest.build_parser()
    args = parser.parse_args(["--llm-enrich-from-report", "/tmp/report.json"])
    assert args.llm_model == ""
    with pytest.raises(SystemExit, match=ingest.LLM_MODEL_ENV):
        ingest.require_llm_model_configured()


def test_calendar_markdown_sanitizer_preserves_hidden_link_targets():
    markdown = (
        "Visible bad hash abcdef1234567890abcdef1234567890abcdef12 should hide, "
        "but [Kanban](blueprints://kanban/items/work-git-feature-fd99339e2122b68b58) "
        "must keep its target. "
        "[blueprints://kanban/items/work-git-feature-fd99339e2122b68b58]"
        "(blueprints://kanban/items/work-git-feature-fd99339e2122b68b58) "
        "must not expose the opaque work item id as visible text."
    )
    clean = ingest.sanitize_calendar_markdown(markdown)
    assert "abcdef1234567890abcdef1234567890abcdef12" not in clean
    assert "[commit reference]" in clean
    assert "blueprints://kanban/items/work-git-feature-fd99339e2122b68b58" in clean
    visible = re.sub(r"\[[^\]]+\]\([^)]+\)", lambda m: m.group(0).split("](")[0], clean)
    assert "fd99339e2122b68b58" not in visible
    assert "[Kanban item](blueprints://kanban/items/work-git-feature-fd99339e2122b68b58)" in clean


def test_visible_identifier_scan_ignores_markdown_link_targets_only():
    hidden_hash = "abcdef1234567890abcdef1234567890abcdef12"
    visible_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    markdown = (
        f"[Readable commit](https://github.com/davros1973/xarta-node/commit/{hidden_hash}) "
        f"but this visible hash {visible_hash} should fail, and "
        "work-git-feature-1234567890abcdef should fail too."
    )

    hits = ingest.visible_calendar_identifier_hits(markdown)
    assert hidden_hash not in hits
    assert visible_hash in hits
    assert "work-git-feature-1234567890abcdef" in hits


def test_markdown_from_llm_summary_accepts_summary_fallback():
    markdown = ingest.markdown_from_llm_summary(
        {"title": "Git activity summary - 2026-06-22", "summary": "Natural English summary."}
    )
    assert markdown.startswith("# Git activity summary - 2026-06-22")
    assert "Natural English summary." in markdown


def test_cached_report_summaries_append_readable_kanban_links_when_llm_omits_them():
    report = _llm_enriched_report()
    report["calendar_summaries"][0]["markdown"] = (
        "# Git Activity - 2026-06-22\n\n"
        "The local LLM wrote useful natural English but forgot the Kanban link."
    )
    report["calendar_summaries"][0]["related_kanban_items"] = []

    records = ingest.records_from_enriched_report(report, require_llm=True)
    summary = records.summaries[0]

    assert "## Kanban Links" in summary.markdown
    assert "[Calendar Git Summary](blueprints://kanban/items/" in summary.markdown
    assert ingest.visible_calendar_identifier_hits(summary.markdown) == []
    assert len(summary.related_kanban_items) == 1

    preflight = ingest.report_for_record_set(
        report,
        records,
        db_path=Path("/tmp/blueprints.db"),
        mode="apply-preflight",
        run_id="git-import-preflight-test",
        started_at="2026-06-23T00:00:00Z",
    )
    assert "## Kanban Links" in preflight["calendar_summaries"][0]["markdown"]


def test_cached_report_summaries_strip_noncanonical_llm_kanban_links():
    report = _llm_enriched_report()
    stale_item_id = "work-git-feature-stale1234567890"
    report["calendar_summaries"][0]["related_kanban_items"] = [stale_item_id]
    report["calendar_summaries"][0]["markdown"] = (
        "# Git Activity - 2026-06-22\n\n"
        "The LLM wrote a useful summary but reused a stale "
        f"[Calendar Git Summary](blueprints://kanban/items/{stale_item_id}) link."
    )

    records = ingest.records_from_enriched_report(report, require_llm=True)
    summary = records.summaries[0]
    canonical_link = ingest.kanban_item_url(summary.related_kanban_items[0])

    assert stale_item_id not in summary.related_kanban_items
    assert stale_item_id not in summary.markdown
    assert canonical_link in summary.markdown
    assert "[Calendar Git Summary](blueprints://kanban/items/" in summary.markdown
    assert ingest.visible_calendar_identifier_hits(summary.markdown) == []


def test_call_llm_json_retries_invalid_json(monkeypatch):
    responses = iter(['{"projects": [', '{"projects": []}'])
    messages_seen = []

    def fake_chat(**kwargs):
        messages_seen.append(kwargs["messages"])
        return next(responses)

    monkeypatch.setattr(ingest, "call_llm_chat", fake_chat)
    parsed = ingest.call_llm_json(
        endpoint=ingest.DEFAULT_LLM_ENDPOINT,
        model=ingest.DEFAULT_LLM_MODEL,
        messages=[
            {"role": "system", "content": "system"},
            {"role": "user", "content": "return json"},
        ],
        max_tokens=100,
        temperature=0,
        timeout=1,
    )
    assert parsed == {"projects": []}
    assert len(messages_seen) == 2
    assert "previous response was invalid" in messages_seen[1][-1]["content"]


def test_cached_report_llm_enrichment_replaces_fallback_summary(monkeypatch):
    repos, commits, features, summaries = _fixture_records()
    report = ingest.build_report(
        mode="dry-run",
        db_path=Path("/tmp/blueprints.db"),
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        tz_name="Etc/UTC",
        existing_git_summary_day=None,
        start_day=date(2026, 6, 22),
        end_day=date(2026, 6, 22),
        owners=["davros1973", "xarta"],
        writable_only=True,
        authors=["davros1973"],
        all_commit_authors=False,
        max_branches=0,
        repo_scope={"visible_total": 1, "allowed_total": 1},
        repos=repos,
        repo_fetch_reports={repos[0].full_name: {"branches": {"truncated": False}, "errors": []}},
        commits=commits,
        features=features,
        summaries=summaries,
        rate_limit_before={},
        include_commit_details=True,
    )
    calls = []

    def fake_llm(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return ingest.json.dumps(
                {
                    "projects": [
                        {
                            "project_key": "blueprints-personal",
                            "project_title": "Blueprints Personal",
                            "subprojects": [
                                {
                                    "subproject_key": "git-activity",
                                    "subproject_title": "Git Activity",
                                    "features": [
                                        {
                                            "feature_key": "calendar-git-summary",
                                            "feature_title": "Calendar Git Summary",
                                            "status": "active",
                                            "notes": "LLM-inferred feature arc.",
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            )
        return ingest.json.dumps(
            {
                "title": "Git activity summary - 2026-06-22",
                "markdown": (
                    "# Git Activity - 2026-06-22\n\n"
                    "Work focused on turning GitHub history into a readable Calendar summary "
                    "and a Kanban feature arc, with idempotency checks and no per-commit noise."
                ),
            }
        )

    monkeypatch.setattr(ingest, "call_llm_chat", fake_llm)
    enriched = ingest.enrich_report_with_local_llm(
        report,
        endpoint=ingest.DEFAULT_LLM_ENDPOINT,
        model=ingest.DEFAULT_LLM_MODEL,
        limit_days=1,
        limit_arc_chunks=1,
        max_commits_per_prompt=50,
        max_tokens=1200,
        temperature=0.1,
        timeout=30,
    )
    assert len(calls) == 2
    assert calls[0]["model"] == ingest.DEFAULT_LLM_MODEL
    assert calls[0]["endpoint"] == ingest.DEFAULT_LLM_ENDPOINT
    enriched_summary = enriched["calendar_summaries"][0]
    assert enriched_summary["summary_source"] == "local_llm"
    assert "fallback_markdown" in enriched_summary
    assert "turning GitHub history into a readable Calendar summary" in enriched_summary["markdown"]
    assert enriched["llm_enrichment"]["arc_context"][0]["project_title"] == "Blueprints Personal"
    assert enriched["kanban_arcs"]
    assert enriched["features"][0]["project_arc_id"]


def test_cached_report_llm_enrichment_checkpoints_and_resumes(tmp_path, monkeypatch):
    report = _fixture_report(include_commit_details=True)
    checkpoint_path = tmp_path / "llm-enriched.json"
    calls = []

    def fake_llm(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return ingest.json.dumps(
                {
                    "projects": [
                        {
                            "project_key": "blueprints-personal",
                            "project_title": "Blueprints Personal",
                            "subprojects": [
                                {
                                    "subproject_key": "git-activity",
                                    "subproject_title": "Git Activity",
                                    "features": [
                                        {
                                            "feature_key": "calendar-git-summary",
                                            "feature_title": "Calendar Git Summary",
                                            "status": "active",
                                            "notes": "LLM-inferred feature arc.",
                                            "evidence_commit_ids": [
                                                commit["commit_id"] for commit in report["commits"]
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            )
        return ingest.json.dumps(
            {
                "title": "Git activity summary - 2026-06-22",
                "markdown": "# Git Activity - 2026-06-22\n\nNatural LLM summary from cached commits.",
            }
        )

    monkeypatch.setattr(ingest, "call_llm_chat", fake_llm)
    enriched = ingest.enrich_report_with_local_llm(
        report,
        endpoint=ingest.DEFAULT_LLM_ENDPOINT,
        model=ingest.DEFAULT_LLM_MODEL,
        limit_days=1,
        limit_arc_chunks=1,
        max_commits_per_prompt=50,
        max_tokens=1200,
        temperature=0.1,
        timeout=30,
        checkpoint_path=checkpoint_path,
    )
    assert len(calls) == 2
    assert checkpoint_path.exists()
    checkpoint = ingest.load_json_report(checkpoint_path)
    assert checkpoint["llm_enrichment"]["status"] == "complete"
    assert checkpoint["calendar_summaries"][0]["summary_source"] == "local_llm"
    assert enriched["llm_enrichment"]["status"] == "complete"

    def fail_llm(**_kwargs):
        pytest.fail("LLM was called despite resume data")

    monkeypatch.setattr(ingest, "call_llm_chat", fail_llm)
    resumed = ingest.enrich_report_with_local_llm(
        checkpoint,
        endpoint=ingest.DEFAULT_LLM_ENDPOINT,
        model=ingest.DEFAULT_LLM_MODEL,
        limit_days=1,
        limit_arc_chunks=1,
        max_commits_per_prompt=50,
        max_tokens=1200,
        temperature=0.1,
        timeout=30,
        checkpoint_path=checkpoint_path,
    )
    assert resumed["calendar_summaries"][0]["summary_source"] == "local_llm"
    assert resumed["llm_enrichment"]["status"] == "complete"


def test_records_from_report_requires_complete_local_llm_summaries():
    report = _fixture_report(include_commit_details=True)
    with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
        ingest.records_from_enriched_report(report, require_llm=True)


def test_records_from_report_requires_calendar_summaries_for_cached_apply():
    report = _llm_enriched_report()
    report["calendar_summaries"] = []
    with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
        ingest.records_from_enriched_report(report, require_llm=True)


def test_records_from_report_requires_summary_for_every_commit_day():
    report = _llm_enriched_report()
    extra_commit = dict(report["commits"][0])
    extra_commit.update(
        {
            "commit_id": "ghc-extra-unsummarized-day",
            "sha": "c" * 40,
            "short_sha": "c" * 7,
            "local_date": "2026-06-21",
            "committed_at": "2026-06-21T12:00:00Z",
            "html_url": "https://github.com/davros1973/xarta-node/commit/" + "c" * 40,
        }
    )
    report["commits"].append(extra_commit)
    report["window"]["date_start"] = "2026-06-21"
    with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
        ingest.records_from_enriched_report(report, require_llm=True)


def test_records_from_report_requires_exact_local_llm_model_and_endpoint():
    for key, value in [
        ("model", "some-other-local-model"),
        ("endpoint", "http://127.0.0.1:9999/not-the-blueprints-proxy"),
    ]:
        report = _llm_enriched_report()
        report["llm_enrichment"][key] = value
        with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
            ingest.records_from_enriched_report(report, require_llm=True)


def test_preflight_apply_from_report_refuses_today_activity_without_db_open(tmp_path, monkeypatch):
    today = ingest.datetime.now(ingest.ZoneInfo("Etc/UTC")).date().isoformat()
    report = _llm_enriched_report()
    report["window"]["date_end"] = today
    for commit in report["commits"]:
        commit["local_date"] = today
        commit["committed_at"] = f"{today}T12:00:00Z"
    report["calendar_summaries"][0]["local_date"] = today
    report["calendar_summaries"][0]["summary_id"] = f"git-day-{today}"
    report["calendar_summaries"][0]["event_id"] = f"git-summary-{today}"
    report_path = tmp_path / "today-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="Cached report includes today/future Git activity"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_preflight_apply_from_report_refuses_out_of_scope_repo_without_db_open(
    tmp_path, monkeypatch
):
    report = _llm_enriched_report()
    report["repos_scanned"][0]["full_name"] = "All-About-AI-YouTube/not-ours"
    report["repos_scanned"][0]["can_push"] = False
    report["commits"][0]["repo_full_name"] = "All-About-AI-YouTube/not-ours"
    report_path = tmp_path / "out-of-scope-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="Cached report includes out-of-scope GitHub activity"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_preflight_apply_from_report_refuses_archived_repo_without_db_open(tmp_path, monkeypatch):
    report = _llm_enriched_report()
    report["repos_scanned"][0]["is_archived"] = True
    report_path = tmp_path / "archived-repo-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="repo davros1973/xarta-node is archived"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_preflight_apply_from_report_refuses_unlisted_commit_repo_without_db_open(
    tmp_path,
    monkeypatch,
):
    report = _llm_enriched_report()
    report["commits"][0]["repo_full_name"] = "davros1973/not-in-scoped-repo-list"
    report_path = tmp_path / "unlisted-repo-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="references unscoped repo"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_preflight_apply_from_report_refuses_blank_calendar_markdown_without_db_open(
    tmp_path,
    monkeypatch,
):
    report = _llm_enriched_report()
    report["calendar_summaries"][0]["markdown"] = " \n\t "
    report_path = tmp_path / "blank-calendar-markdown-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_preflight_apply_from_report_refuses_visible_long_markdown_identifier_without_db_open(
    tmp_path,
    monkeypatch,
):
    report = _llm_enriched_report()
    report["calendar_summaries"][0]["markdown"] = (
        "# Git Activity\n\n"
        "The local LLM accidentally exposed abcdef1234567890abcdef1234567890abcdef12."
    )
    report_path = tmp_path / "visible-long-identifier-enriched.json"
    report_path.write_text(ingest.json.dumps(report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires complete local-LLM enrichment"):
        ingest.main(["--preflight-apply-from-report", str(report_path), "--quiet"])


def test_apply_from_enriched_report_creates_llm_kanban_hierarchy_idempotently(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    conn = _conn()
    report = _llm_enriched_report()
    records = ingest.records_from_enriched_report(report, require_llm=True)
    assert len(records.kanban_arcs) == 2
    assert len(records.features) == 1
    kwargs = dict(
        conn=conn,
        repos=records.repos,
        commits=records.commits,
        features=records.features,
        kanban_arcs=records.kanban_arcs,
        summaries=records.summaries,
        start_day=records.start_day,
        end_day=records.end_day,
        tz_name=records.tz_name,
        run_id="git-import-apply-report-test",
        started_at="2026-06-23T02:00:00Z",
        params=records.params,
        report=report,
        apply_mode="apply-from-report",
    )
    ingest.apply_ingest(**kwargs)
    conn.commit()
    first_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in [
            "personal_git_kanban_arcs",
            "personal_git_features",
            "personal_git_daily_summaries",
            "personal_git_import_runs",
            "personal_events",
            "kanban_items",
        ]
    }
    ingest.apply_ingest(**kwargs)
    conn.commit()
    second_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in first_counts
    }
    assert second_counts == first_counts

    project = conn.execute(
        "SELECT * FROM kanban_items WHERE item_type='project' AND parent_item_id=?",
        (ingest.ROOT_WORK_ITEM_ID,),
    ).fetchone()
    subproject = conn.execute("SELECT * FROM kanban_items WHERE item_type='subproject'").fetchone()
    feature = conn.execute("SELECT * FROM kanban_items WHERE item_type='feature'").fetchone()
    assert project["title"] == "Blueprints Personal"
    assert project["depth"] == 1
    assert subproject["title"] == "Git Activity"
    assert subproject["parent_item_id"] == project["item_id"]
    assert subproject["depth"] == 2
    assert feature["title"] == "Calendar Git Summary"
    assert feature["parent_item_id"] == subproject["item_id"]
    assert feature["depth"] == 3

    event = conn.execute(
        "SELECT * FROM personal_events WHERE event_id='git-summary-2026-06-22'"
    ).fetchone()
    summary = conn.execute(
        "SELECT * FROM personal_git_daily_summaries WHERE summary_id='git-day-2026-06-22'"
    ).fetchone()
    assert event["source_type"] == "git"
    assert event["kind"] == "git-summary"
    assert event["source_ref"] == "personal_git_daily_summaries:git-day-2026-06-22"
    assert event["content_projection"] == summary["markdown"]
    assert (
        "GitHub history was folded into a readable Calendar summary" in event["content_projection"]
    )
    assert ingest.visible_calendar_identifier_hits(event["content_projection"]) == []
    assert "fallback scaffold" not in event["content_projection"].lower()
    summary_provenance = ingest.json.loads(summary["provenance_json"])
    assert summary_provenance["summary_source"] == "local_llm"
    assert summary_provenance["repo_full_names"] == ["davros1973/xarta-node"]
    assert summary_provenance["commit_ids"] == [commit["commit_id"] for commit in report["commits"]]
    assert summary_provenance["commit_urls"] == [commit["html_url"] for commit in report["commits"]]
    summary_related_kanban_items = ingest.json.loads(summary["related_kanban_items_json"])
    assert summary_provenance["kanban_links"] == [
        ingest.kanban_item_url(item_id) for item_id in summary_related_kanban_items
    ]
    assert ingest.kanban_item_url(feature["item_id"]) in summary_provenance["kanban_links"]
    assert "git" in ingest.json.loads(event["tags_json"])
    related_kanban_items = ingest.json.loads(event["related_kanban_items_json"])
    assert feature["item_id"] in related_kanban_items
    db_refs = ingest.json.loads(event["db_refs_json"])
    assert "personal_git_daily_summaries:git-day-2026-06-22" in db_refs
    assert f"kanban_items:{feature['item_id']}" in db_refs
    provenance = ingest.json.loads(event["provenance_json"])
    assert provenance["git"]["summary_source"] == "local_llm"
    assert provenance["link_schema"] == "blueprints://kanban/items/<item_id>"
    assert "blueprints://kanban/items/" in event["content_projection"]


def test_apply_ingest_stages_git_cache_before_kanban_calendar_and_import_run(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    conn = _conn()
    report = _llm_enriched_report()
    records = ingest.records_from_enriched_report(report, require_llm=True)
    calls = []

    for helper_name in [
        "upsert_source",
        "upsert_repo",
        "upsert_commit",
        "upsert_root_work_item",
        "upsert_kanban_arc",
        "upsert_arc_work_item",
        "upsert_feature",
        "upsert_feature_work_item",
        "upsert_daily_summary",
        "upsert_import_run",
    ]:
        original = getattr(ingest, helper_name)

        def wrapper(*args, __helper_name=helper_name, __original=original, **kwargs):
            calls.append(__helper_name)
            return __original(*args, **kwargs)

        monkeypatch.setattr(ingest, helper_name, wrapper)

    ingest.apply_ingest(
        conn=conn,
        repos=records.repos,
        commits=records.commits,
        features=records.features,
        kanban_arcs=records.kanban_arcs,
        summaries=records.summaries,
        start_day=records.start_day,
        end_day=records.end_day,
        tz_name=records.tz_name,
        run_id="git-import-order-test",
        started_at="2026-06-23T02:00:00Z",
        params=records.params,
        report=report,
        apply_mode="apply-from-report",
    )

    first = {name: calls.index(name) for name in set(calls)}
    last = {name: len(calls) - 1 - calls[::-1].index(name) for name in set(calls)}
    assert first["upsert_source"] < first["upsert_repo"] < first["upsert_commit"]
    assert last["upsert_commit"] < first["upsert_root_work_item"]
    assert first["upsert_root_work_item"] < first["upsert_kanban_arc"]
    assert last["upsert_arc_work_item"] < first["upsert_feature"]
    assert first["upsert_feature"] < first["upsert_feature_work_item"]
    assert last["upsert_feature_work_item"] < first["upsert_daily_summary"]
    assert last["upsert_daily_summary"] < first["upsert_import_run"]


def test_apply_from_report_cli_requires_explicit_apply_before_db_open(tmp_path, monkeypatch):
    report_path = tmp_path / "enriched.json"
    report_path.write_text(ingest.json.dumps(_llm_enriched_report()), encoding="utf-8")
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires --apply"):
        ingest.main(["--apply-from-report", str(report_path)])


def test_apply_from_report_cli_requires_approved_preflight_before_db_open(tmp_path, monkeypatch):
    report_path = tmp_path / "enriched.json"
    report_path.write_text(ingest.json.dumps(_llm_enriched_report()), encoding="utf-8")
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires --approved-preflight-report"):
        ingest.main(["--apply", "--apply-from-report", str(report_path)])


def test_apply_from_report_cli_requires_approved_preflight_digest_before_db_open(
    tmp_path,
    monkeypatch,
):
    source_report = _llm_enriched_report()
    report_path = tmp_path / "enriched.json"
    preflight_path = tmp_path / "approved-preflight.json"
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    _write_approved_preflight_report(source_report, report_path, preflight_path)
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="requires --approved-preflight-digest"):
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(preflight_path),
            ]
        )


def test_preflight_apply_from_report_validates_without_db_open(tmp_path, monkeypatch):
    report_path = tmp_path / "enriched.json"
    preflight_path = tmp_path / "preflight.json"
    review_path = tmp_path / "preflight.md"
    report_path.write_text(ingest.json.dumps(_llm_enriched_report()), encoding="utf-8")
    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )

    assert (
        ingest.main(
            [
                "--preflight-apply-from-report",
                str(report_path),
                "--report",
                str(preflight_path),
                "--review-report",
                str(review_path),
                "--quiet",
            ]
        )
        == 0
    )
    preflight = ingest.load_json_report(preflight_path)
    assert preflight["mode"] == "apply-preflight"
    assert preflight["planned_writes"]["personal_git_kanban_arcs"] == 2
    assert preflight["planned_writes"]["personal_events"] == 1
    assert preflight["sync_queue_plan"]["source_row_count"] == 14
    assert preflight["apply_safety_plan"]["backup_flag"] == "--backup-before-apply"
    assert (
        preflight["apply_safety_plan"]["required_approval_digest_flag"]
        == "--approved-preflight-digest"
    )
    assert preflight["apply_safety_plan"]["github_calls_during_apply"] is False
    assert preflight["apply_safety_plan"]["llm_calls_during_apply"] is False
    assert preflight["acceptance_checks"]["all_passed"] is True
    assert {
        check["check_id"]: check["status"] for check in preflight["acceptance_checks"]["checks"]
    }["calendar_markdown"] == "pass"
    assert preflight["approval"]["approval_type"] == "github-activity-apply-preflight"
    assert preflight["approval"]["approval_digest"]
    review = review_path.read_text(encoding="utf-8")
    assert "Apply preflight only. No Blueprints database writes" in review
    assert "Source rows to queue per apply: 14" in review
    assert "## Apply Safety Plan" in review
    assert "Recommended backup flag: `--backup-before-apply`" in review
    assert "Required approval digest flag: `--approved-preflight-digest`" in review
    assert "## Preflight Acceptance Checks" in review


def test_apply_from_report_refuses_mismatched_approved_preflight_before_db_open(
    tmp_path,
    monkeypatch,
):
    original_report = _llm_enriched_report()
    approved_source_path = tmp_path / "approved-source.json"
    approved_preflight_path = tmp_path / "approved-preflight.json"
    approved_source_path.write_text(ingest.json.dumps(original_report), encoding="utf-8")
    approved_preflight = _write_approved_preflight_report(
        original_report,
        approved_source_path,
        approved_preflight_path,
    )

    changed_report = _llm_enriched_report()
    changed_report["calendar_summaries"][0]["markdown"] += "\n\nAdditional reviewed sentence."
    changed_report_path = tmp_path / "changed-source.json"
    changed_report_path.write_text(ingest.json.dumps(changed_report), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="does not match the apply source report"):
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(changed_report_path),
                "--approved-preflight-report",
                str(approved_preflight_path),
                "--approved-preflight-digest",
                _preflight_digest(approved_preflight),
                "--quiet",
            ]
        )


def test_apply_from_report_refuses_tampered_approved_preflight_before_db_open(
    tmp_path,
    monkeypatch,
):
    source_report = _llm_enriched_report()
    report_path = tmp_path / "source.json"
    approved_preflight_path = tmp_path / "approved-preflight.json"
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    preflight = _write_approved_preflight_report(
        source_report,
        report_path,
        approved_preflight_path,
    )
    preflight["calendar_summaries"][0]["markdown"] += "\n\nTampered after review."
    approved_preflight_path.write_text(ingest.json.dumps(preflight), encoding="utf-8")

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="approval digest does not match its current contents"):
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(approved_preflight_path),
                "--approved-preflight-digest",
                _preflight_digest(preflight),
                "--quiet",
            ]
        )


def test_apply_from_report_refuses_mismatched_operator_digest_before_db_open(
    tmp_path,
    monkeypatch,
):
    source_report = _llm_enriched_report()
    report_path = tmp_path / "source.json"
    approved_preflight_path = tmp_path / "approved-preflight.json"
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    _write_approved_preflight_report(
        source_report,
        report_path,
        approved_preflight_path,
    )

    monkeypatch.setattr(
        ingest, "connect_db", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="does not match the reviewed preflight digest"):
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(approved_preflight_path),
                "--approved-preflight-digest",
                "not-the-approved-digest",
                "--quiet",
            ]
        )


def test_apply_from_report_cli_is_cache_only_and_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    conn = _conn()
    conn.executemany(
        "INSERT INTO nodes(node_id) VALUES (?)",
        [("test-node",), ("peer-a",), ("peer-b",)],
    )
    report_path = tmp_path / "enriched.json"
    preflight_path = tmp_path / "approved-preflight.json"
    apply_path = tmp_path / "apply.json"
    review_path = tmp_path / "apply.md"
    source_report = _llm_enriched_report()
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    preflight = _write_approved_preflight_report(source_report, report_path, preflight_path)

    monkeypatch.setattr(ingest, "connect_db", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )
    monkeypatch.setattr(
        ingest, "fetch_rate_limit", lambda: pytest.fail("GitHub rate limit was checked")
    )
    monkeypatch.setattr(
        ingest,
        "enumerate_repositories",
        lambda **_kwargs: pytest.fail("GitHub repositories were enumerated"),
    )
    monkeypatch.setattr(ingest, "call_llm_chat", lambda **_kwargs: pytest.fail("LLM was called"))

    args = [
        "--apply",
        "--apply-from-report",
        str(report_path),
        "--approved-preflight-report",
        str(preflight_path),
        "--approved-preflight-digest",
        _preflight_digest(preflight),
        "--report",
        str(apply_path),
        "--review-report",
        str(review_path),
        "--quiet",
    ]
    assert ingest.main(args) == 0
    first_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in [
            "personal_git_repositories",
            "personal_git_commits",
            "personal_git_kanban_arcs",
            "personal_git_features",
            "personal_git_daily_summaries",
            "personal_git_import_runs",
            "personal_events",
            "kanban_items",
        ]
    }
    assert ingest.main(args) == 0
    second_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in first_counts
    }
    assert second_counts == first_counts

    apply_report = ingest.load_json_report(apply_path)
    assert apply_report["mode"] == "apply-from-report"
    assert apply_report["planned_writes"]["personal_events"] == 1
    assert apply_report["sync_queue_plan"]["source_row_count"] == 14
    assert apply_report["sync_queue_plan"]["target_node_count"] == 2
    assert apply_report["sync_queue_plan"]["actual_queue_entries_added"] == 28
    assert apply_report["apply_safety_plan"]["backup_flag"] == "--backup-before-apply"
    assert (
        apply_report["apply_safety_plan"]["required_approval_digest_flag"]
        == "--approved-preflight-digest"
    )
    assert apply_report["llm_enrichment"]["source"] == "local_llm"
    review = review_path.read_text(encoding="utf-8")
    assert "Apply report. This run wrote approved Blueprints records." in review
    assert "Actual sync queue entries added: 28" in review


def test_enqueue_for_peers_loads_node_id_from_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("BLUEPRINTS_NODE_ID", raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text("BLUEPRINTS_NODE_ID=test-node\n", encoding="utf-8")
    monkeypatch.setenv("BLUEPRINTS_ENV_FILE", str(env_path))
    conn = _conn()
    conn.executemany(
        "INSERT INTO nodes(node_id) VALUES (?)",
        [("test-node",), ("peer-a",), ("peer-b",)],
    )

    ingest.enqueue_for_peers(
        conn,
        "UPDATE",
        "personal_events",
        "evt-1",
        {"event_id": "evt-1"},
        1,
    )

    targets = [
        row["target_node_id"]
        for row in conn.execute("SELECT target_node_id FROM sync_queue ORDER BY target_node_id")
    ]
    assert targets == ["peer-a", "peer-b"]


def test_enqueue_for_peers_refuses_unknown_self_node(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "missing-node")
    conn = _conn()
    conn.executemany(
        "INSERT INTO nodes(node_id) VALUES (?)",
        [("test-node",), ("peer-a",)],
    )

    with pytest.raises(RuntimeError, match="not present in the nodes table"):
        ingest.enqueue_for_peers(
            conn,
            "UPDATE",
            "personal_events",
            "evt-1",
            {"event_id": "evt-1"},
            1,
        )


def test_apply_from_report_can_backup_sqlite_before_apply(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    db_path = tmp_path / "blueprints.db"
    backup_path = tmp_path / "blueprints-before-git-import.sqlite"
    seed_conn = _conn()
    disk_conn = sqlite3.connect(db_path)
    try:
        seed_conn.backup(disk_conn)
    finally:
        disk_conn.close()
        seed_conn.close()

    report_path = tmp_path / "enriched.json"
    preflight_path = tmp_path / "approved-preflight.json"
    apply_path = tmp_path / "apply.json"
    review_path = tmp_path / "apply.md"
    source_report = _llm_enriched_report()
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    preflight = _write_approved_preflight_report(source_report, report_path, preflight_path)

    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )
    monkeypatch.setattr(
        ingest, "fetch_rate_limit", lambda: pytest.fail("GitHub rate limit was checked")
    )
    monkeypatch.setattr(ingest, "call_llm_chat", lambda **_kwargs: pytest.fail("LLM was called"))

    assert (
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(preflight_path),
                "--approved-preflight-digest",
                _preflight_digest(preflight),
                "--db-path",
                str(db_path),
                "--backup-before-apply",
                "--backup-path",
                str(backup_path),
                "--report",
                str(apply_path),
                "--review-report",
                str(review_path),
                "--quiet",
            ]
        )
        == 0
    )

    assert backup_path.exists()
    backup_conn = sqlite3.connect(backup_path)
    backup_conn.row_factory = sqlite3.Row
    try:
        row = backup_conn.execute(
            "SELECT COUNT(*) AS c FROM personal_events WHERE source_type='git'"
        ).fetchone()
        assert row["c"] == 0
    finally:
        backup_conn.close()

    applied_conn = sqlite3.connect(db_path)
    applied_conn.row_factory = sqlite3.Row
    try:
        row = applied_conn.execute(
            "SELECT COUNT(*) AS c FROM personal_events WHERE source_type='git'"
        ).fetchone()
        assert row["c"] == 1
    finally:
        applied_conn.close()

    apply_report = ingest.load_json_report(apply_path)
    assert apply_report["database_backup"]["path"] == str(backup_path)
    assert apply_report["database_backup"]["size_bytes"] > 0
    review = review_path.read_text(encoding="utf-8")
    assert "## Database Backup" in review
    assert f"Path: `{backup_path}`" in review


def test_verify_apply_from_report_cli_is_read_only_and_passes_after_apply(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    db_path = tmp_path / "blueprints.db"
    seed_conn = _conn()
    disk_conn = sqlite3.connect(db_path)
    try:
        seed_conn.backup(disk_conn)
    finally:
        disk_conn.close()
        seed_conn.close()

    report_path = tmp_path / "enriched.json"
    preflight_path = tmp_path / "approved-preflight.json"
    apply_path = tmp_path / "apply.json"
    verify_path = tmp_path / "verify.json"
    verify_review_path = tmp_path / "verify.md"
    source_report = _llm_enriched_report()
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    preflight = _write_approved_preflight_report(source_report, report_path, preflight_path)

    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )
    monkeypatch.setattr(
        ingest, "fetch_rate_limit", lambda: pytest.fail("GitHub rate limit was checked")
    )
    monkeypatch.setattr(ingest, "call_llm_chat", lambda **_kwargs: pytest.fail("LLM was called"))

    assert (
        ingest.main(
            [
                "--apply",
                "--apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(preflight_path),
                "--approved-preflight-digest",
                _preflight_digest(preflight),
                "--db-path",
                str(db_path),
                "--report",
                str(apply_path),
                "--quiet",
            ]
        )
        == 0
    )

    unrelated_conn = sqlite3.connect(db_path)
    unrelated_conn.row_factory = sqlite3.Row
    try:
        unrelated_conn.execute(
            """
            INSERT INTO personal_events (
                event_id, source_type, kind, title, local_date, tags_json
            )
            VALUES ('manual-event', 'manual', 'note', 'Manual event', '2026-06-22', '["calendar"]')
            """
        )
        unrelated_conn.execute(
            """
            INSERT INTO kanban_items (
                item_id, title, item_type, source_type, tags_json
            )
            VALUES ('manual-kanban-item', 'Manual work item', 'project', 'manual', '["kanban"]')
            """
        )
        unrelated_conn.commit()
    finally:
        unrelated_conn.close()

    before_conn = sqlite3.connect(db_path)
    before_conn.row_factory = sqlite3.Row
    try:
        queue_before = ingest.sync_queue_count(before_conn)
    finally:
        before_conn.close()

    assert (
        ingest.main(
            [
                "--verify-apply-from-report",
                str(report_path),
                "--approved-preflight-report",
                str(preflight_path),
                "--db-path",
                str(db_path),
                "--report",
                str(verify_path),
                "--review-report",
                str(verify_review_path),
                "--quiet",
            ]
        )
        == 0
    )

    after_conn = sqlite3.connect(db_path)
    after_conn.row_factory = sqlite3.Row
    try:
        assert ingest.sync_queue_count(after_conn) == queue_before
    finally:
        after_conn.close()

    verify_report = ingest.load_json_report(verify_path)
    assert verify_report["mode"] == "apply-verification"
    assert verify_report["post_apply_verification"]["all_passed"] is True
    assert all(
        check["status"] == "pass" for check in verify_report["post_apply_verification"]["checks"]
    )
    assert verify_report["post_apply_verification"]["expected_counts"]["personal_events"] == 1
    assert verify_report["post_apply_verification"]["actual_counts"]["personal_events"] == 1
    assert (
        verify_report["post_apply_verification"]["expected_counts"]["personal_git_import_runs"] == 1
    )
    assert (
        verify_report["post_apply_verification"]["actual_counts"]["personal_git_import_runs"] == 1
    )
    assert verify_report["post_apply_verification"]["expected_counts"]["kanban_items"] == 4
    assert verify_report["post_apply_verification"]["actual_counts"]["kanban_items"] == 4
    review = verify_review_path.read_text(encoding="utf-8")
    assert "Post-apply verification only" in review
    assert "## Post-Apply Verification Checks" in review
    assert "Expected rows present: PASS" in review


def test_verify_apply_from_report_cli_reports_missing_rows_without_writing(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "blueprints.db"
    seed_conn = _conn()
    disk_conn = sqlite3.connect(db_path)
    try:
        seed_conn.backup(disk_conn)
    finally:
        disk_conn.close()
        seed_conn.close()

    report_path = tmp_path / "enriched.json"
    verify_path = tmp_path / "verify.json"
    source_report = _llm_enriched_report()
    report_path.write_text(ingest.json.dumps(source_report), encoding="utf-8")
    monkeypatch.setattr(
        ingest, "run_gh", lambda *_args, **_kwargs: pytest.fail("GitHub was called")
    )
    monkeypatch.setattr(ingest, "call_llm_chat", lambda **_kwargs: pytest.fail("LLM was called"))

    assert (
        ingest.main(
            [
                "--verify-apply-from-report",
                str(report_path),
                "--db-path",
                str(db_path),
                "--report",
                str(verify_path),
                "--quiet",
            ]
        )
        == 1
    )
    verify_report = ingest.load_json_report(verify_path)
    verification = verify_report["post_apply_verification"]
    assert verification["all_passed"] is False
    by_id = {check["check_id"]: check for check in verification["checks"]}
    assert by_id["expected_rows_present"]["status"] == "fail"
    assert by_id["canonical_row_minimums"]["status"] == "fail"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM personal_events").fetchone()
        assert row["c"] == 0
    finally:
        conn.close()


def test_verify_apply_from_report_refuses_apply_flag_before_db_open(tmp_path, monkeypatch):
    report_path = tmp_path / "enriched.json"
    report_path.write_text(ingest.json.dumps(_llm_enriched_report()), encoding="utf-8")
    monkeypatch.setattr(
        ingest, "connect_db_readonly", lambda *_args, **_kwargs: pytest.fail("DB was opened")
    )
    with pytest.raises(SystemExit, match="read-only"):
        ingest.main(["--apply", "--verify-apply-from-report", str(report_path)])


def test_apply_ingest_is_idempotent_for_cache_calendar_and_kanban_rows(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_NODE_ID", "test-node")
    conn = _conn()
    repos, commits, features, summaries = _fixture_records()
    kwargs = dict(
        conn=conn,
        repos=repos,
        commits=commits,
        features=features,
        summaries=summaries,
        start_day=date(2026, 6, 22),
        end_day=date(2026, 6, 22),
        tz_name="Etc/UTC",
        run_id="git-import-test",
        started_at="2026-06-23T00:00:00Z",
        params={"test": True},
        report={"test": True},
    )
    ingest.apply_ingest(**kwargs)
    conn.commit()
    first_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in [
            "personal_git_repositories",
            "personal_git_commits",
            "personal_git_features",
            "personal_git_daily_summaries",
            "personal_git_import_runs",
            "personal_events",
            "kanban_items",
        ]
    }
    ingest.apply_ingest(**kwargs)
    conn.commit()
    second_counts = {
        table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        for table in first_counts
    }
    assert second_counts == first_counts
    event = conn.execute(
        "SELECT * FROM personal_events WHERE event_id='git-summary-2026-06-22'"
    ).fetchone()
    assert event["source_type"] == "git"
    assert event["kind"] == "git-summary"
    assert "git" in event["tags_json"]
    assert "blueprints://kanban/items/" in event["content_projection"]
