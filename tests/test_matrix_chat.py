import sys
from pathlib import Path

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
    assert joined[0]["last_preview"] == "brief reply"
    assert invited == []
    assert message["body"] == "brief reply"
    assert "access_token" not in rendered.lower()
    assert "password" not in rendered.lower()
    assert "authorization" not in rendered.lower()
