import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_matrix_chat as matrix_chat


def test_matrix_chat_reads_private_env_without_exposing_token(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=secret-token-value",
                "MATRIX_HERMES_SMOKE_ROOM_ID=!room:test.example",
                "MATRIX_HERMES_USER_ID=@hermes:test.example",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))
    monkeypatch.delenv("MATRIX_CHAT_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MATRIX_CHAT_USER_ID", raising=False)

    settings = matrix_chat._settings()
    status = {
        "configured": bool(settings["user_id"] and settings["access_token"]),
        "homeserver_url": settings["public_homeserver"],
        "user_id": settings["user_id"],
        "default_room_id": settings["smoke_room_id"],
        "hermes_user_id": settings["hermes_user_id"],
    }

    assert settings["access_token"] == "secret-token-value"
    assert status == {
        "configured": True,
        "homeserver_url": "https://matrix.local",
        "user_id": "@codex:test.example",
        "default_room_id": "!room:test.example",
        "hermes_user_id": "@hermes:test.example",
    }
    assert "secret-token-value" not in repr(status)


def test_matrix_chat_room_and_message_mapping_do_not_return_credentials():
    sync = {
        "next_batch": "s123",
        "rooms": {
            "join": {
                "!room:test.example": {
                    "summary": {"m.joined_member_count": 2},
                    "state": {
                        "events": [
                            {
                                "type": "m.room.name",
                                "content": {"name": "Hermes Local Smoke"},
                            }
                        ]
                    },
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "event_id": "$event1",
                                "sender": "@hermes:test.example",
                                "origin_server_ts": 1710000000000,
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "brief reply",
                                },
                            }
                        ]
                    },
                }
            },
            "invite": {},
        },
    }

    joined, invited = matrix_chat._rooms_from_sync(sync)
    message = matrix_chat._message_from_event(
        sync["rooms"]["join"]["!room:test.example"]["timeline"]["events"][0],
        "!room:test.example",
    )
    rendered = repr({"joined": joined, "invited": invited, "message": message})

    assert joined[0]["name"] == "Hermes Local Smoke"
    assert joined[0]["display_name"] == "Hermes Local Smoke"
    assert joined[0]["name_source"] == "m.room.name"
    assert joined[0]["last_preview"] == "brief reply"
    assert invited == []
    assert message["body"] == "brief reply"
    assert "access_token" not in rendered.lower()
    assert "password" not in rendered.lower()
    assert "authorization" not in rendered.lower()


def test_matrix_chat_room_mapping_marks_missing_names_as_fallback():
    sync = {
        "rooms": {
            "join": {
                "!roomwithnoname:test.example": {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "event_id": "$event2",
                                "sender": "@hermes:test.example",
                                "origin_server_ts": 1710000000001,
                                "content": {"msgtype": "m.text", "body": "hello"},
                            }
                        ]
                    }
                }
            }
        }
    }

    joined, _ = matrix_chat._rooms_from_sync(sync)

    assert joined[0]["name"] == "!roomwithnoname:test.example"
    assert joined[0]["display_name"].startswith("Unnamed room (")
    assert joined[0]["name_source"] == "fallback_room_id"


def test_matrix_chat_invite_candidate_filter_excludes_members_self_and_admin():
    candidates = [
        {
            "user_id": "@codex:test.example",
            "display_name": "codex",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@hermes:test.example",
            "display_name": "hermes",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@operator:test.example",
            "display_name": "operator",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@admin:test.example",
            "display_name": "admin",
            "is_admin": True,
            "deactivated": False,
        },
        {
            "user_id": "@old:test.example",
            "display_name": "old",
            "is_admin": False,
            "deactivated": True,
        },
    ]

    filtered = matrix_chat._filter_invite_candidates(
        candidates,
        excluded_user_ids={"@hermes:test.example"},
        current_user_id="@codex:test.example",
        query="@",
    )

    assert filtered == [{"user_id": "@operator:test.example", "display_name": "operator"}]


def test_matrix_chat_invite_candidate_filter_applies_query():
    candidates = [
        {
            "user_id": "@xarta-operator:test.example",
            "display_name": "xarta-operator",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@hermes:test.example",
            "display_name": "hermes",
            "is_admin": False,
            "deactivated": False,
        },
    ]

    filtered = matrix_chat._filter_invite_candidates(
        candidates,
        excluded_user_ids=set(),
        current_user_id="@codex:test.example",
        query="oper",
    )

    assert filtered == [
        {
            "user_id": "@xarta-operator:test.example",
            "display_name": "xarta-operator",
        }
    ]


def test_matrix_chat_admin_status_does_not_expose_token():
    settings = {
        "public_homeserver": "https://chat.test.example",
        "admin_user_id": "@synapse-admin:test.example",
        "admin_access_token": "admin-token-secret",
    }

    status = matrix_chat._admin_status_payload(settings, reachable=True, health="ok")
    rendered = repr(status)

    assert status["configured"] is True
    assert status["admin_configured"] is True
    assert status["admin_user_id"] == "@synapse-admin:test.example"
    assert status["features"] == {
        "generic_admin_proxy": False,
        "destructive_actions": False,
    }
    assert "admin-token-secret" not in rendered
    assert "admin_access_token" not in rendered
    assert "access_token" not in rendered


def test_matrix_chat_admin_user_dto_drops_secret_material():
    user = matrix_chat._normalize_admin_user(
        {
            "name": "@operator:test.example",
            "displayname": "operator",
            "admin": False,
            "deactivated": False,
            "is_guest": False,
            "creation_ts": 1770000000000,
            "access_token": "token-secret",
            "password": "password-secret",
            "pusher": "pusher-secret",
            "topic": "topic-secret",
            "recovery_key": "recovery-secret",
        }
    )
    rendered = repr(user)

    assert user == {
        "user_id": "@operator:test.example",
        "display_name": "operator",
        "is_admin": False,
        "deactivated": False,
        "is_guest": False,
        "creation_ts": 1770000000000,
    }
    for forbidden in (
        "token-secret",
        "password-secret",
        "pusher-secret",
        "topic-secret",
        "recovery-secret",
        "access_token",
        "password",
        "pusher",
        "topic",
        "recovery",
    ):
        assert forbidden not in rendered


def test_matrix_chat_admin_room_dto_handles_missing_name_and_drops_topic():
    room = matrix_chat._normalize_admin_room(
        {
            "room_id": "!room:test.example",
            "joined_members": "3",
            "joined_local_members": 2,
            "version": 10,
            "federatable": "false",
            "public": None,
            "topic": "secret-topic",
        }
    )
    rendered = repr(room)

    assert room == {
        "room_id": "!room:test.example",
        "name": "",
        "canonical_alias": "",
        "joined_members": 3,
        "joined_local_members": 2,
        "version": "10",
        "encrypted": False,
        "public": False,
        "federatable": False,
    }
    assert "secret-topic" not in rendered
    assert "topic" not in rendered


@pytest.mark.asyncio
async def test_matrix_chat_admin_endpoints_fail_when_admin_token_missing(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=chat-token",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))
    monkeypatch.delenv("MATRIX_CHAT_ADMIN_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MATRIX_ADMIN_ACCESS_TOKEN", raising=False)

    with pytest.raises(matrix_chat.HTTPException) as exc:
        await matrix_chat.matrix_chat_admin_users()

    assert exc.value.status_code == 503
    assert "admin token" in exc.value.detail.lower()


def test_matrix_chat_admin_member_reduction_includes_power_without_raw_state():
    state_rows = matrix_chat._room_member_rows_from_state(
        [
            {
                "type": "m.room.power_levels",
                "content": {"users": {"@admin:test.example": 100}},
            },
            {
                "type": "m.room.member",
                "state_key": "@admin:test.example",
                "content": {"membership": "join", "displayname": "Synapse Admin"},
            },
        ]
    )
    member = matrix_chat._normalize_admin_member("@admin:test.example", state_rows)

    assert member == {
        "user_id": "@admin:test.example",
        "membership": "join",
        "display_name": "Synapse Admin",
        "power_level": 100,
    }
    assert "content" not in repr(member)
    assert "state_key" not in repr(member)


@pytest.mark.asyncio
async def test_matrix_chat_create_room_can_request_encryption(monkeypatch):
    captured = {}

    async def fake_matrix_request(method, path, *, json_body=None, **_kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        return {"room_id": "!encrypted:test.example"}

    monkeypatch.setattr(matrix_chat, "_matrix_request", fake_matrix_request)

    result = await matrix_chat.matrix_chat_create_room(
        matrix_chat._CreateRoomBody(name="Encrypted Ops", encrypted=True)
    )

    assert result == {"room_id": "!encrypted:test.example"}
    assert captured["method"] == "POST"
    assert captured["path"] == "/createRoom"
    assert captured["json_body"]["visibility"] == "private"
    assert captured["json_body"]["initial_state"] == [
        {
            "type": "m.room.encryption",
            "state_key": "",
            "content": {"algorithm": "m.megolm.v1.aes-sha2"},
        }
    ]
