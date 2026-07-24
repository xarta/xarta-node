import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).parents[1]
SETUP = ROOT / "setup-syncthing.sh"


def extract_patcher(tmp_path: Path) -> Path:
    source = SETUP.read_text(encoding="utf-8")
    match = re.search(
        r"cat > \"\$TMPPY\" << 'PYEOF'\n(?P<body>.*?)\nPYEOF",
        source,
        flags=re.DOTALL,
    )
    assert match is not None
    patcher = tmp_path / "syncthing-patch.py"
    patcher.write_text(match.group("body"), encoding="utf-8")
    return patcher


def run_patcher(tmp_path: Path, node_id: str) -> ET.Element:
    patcher = extract_patcher(tmp_path)
    config = tmp_path / f"{node_id}-config.xml"
    config.write_text(
        '<?xml version="1.0" encoding="utf-8"?><configuration><gui/><options/></configuration>',
        encoding="utf-8",
    )
    nodes = {
        "nodes": [
            {
                "node_id": "hub-node",
                "primary_ip": "192.0.2.1",
                "syncthing_device_id": "HUB-ID",
            },
            {
                "node_id": "spoke-node-a",
                "primary_ip": "192.0.2.2",
                "syncthing_device_id": "SPOKE-2-ID",
            },
            {
                "node_id": "spoke-node-b",
                "primary_ip": "192.0.2.3",
                "syncthing_device_id": "SPOKE-3-ID",
            },
        ]
    }
    nodes_path = tmp_path / "nodes.json"
    nodes_path.write_text(json.dumps(nodes), encoding="utf-8")
    own_id = {
        "hub-node": "HUB-ID",
        "spoke-node-a": "SPOKE-2-ID",
    }[node_id]
    extras = [
        {"device_id": "MISSMARPLE-ID", "name": "MissMarple"},
        {"device_id": "SHERLOCK-ID", "name": "sherlock-lin"},
        {"device_id": "BERGERAC-ID", "name": "Bergerac"},
    ]
    extra_folders = {
        "xarta-node-skills": [
            "MISSMARPLE-ID",
            "SHERLOCK-ID",
            "BERGERAC-ID",
        ]
    }
    subprocess.run(
        [
            sys.executable,
            str(patcher),
            str(config),
            str(nodes_path),
            node_id,
            own_id,
            "admin",
            "bcrypt-hash",
            "api-key",
            "/srv/assets",
            "/srv/docs",
            "/srv/skills",
            "/srv/voices",
            "/srv/interests",
            "[]",
            json.dumps(extras),
            json.dumps(extra_folders),
            "hub-node",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return ET.parse(config).getroot()


def skills_folder(root: ET.Element) -> ET.Element:
    folder = root.find('folder[@id="xarta-node-skills"]')
    assert folder is not None
    return folder


def test_hub_membership_includes_all_fleet_and_external_spokes(tmp_path):
    folder = skills_folder(run_patcher(tmp_path, "hub-node"))
    assert folder.get("type") == "sendreceive"
    assert folder.get("path") == "/srv/skills"
    assert {device.get("id") for device in folder.findall("device")} == {
        "HUB-ID",
        "SPOKE-2-ID",
        "SPOKE-3-ID",
        "MISSMARPLE-ID",
        "SHERLOCK-ID",
        "BERGERAC-ID",
    }
    versioning = folder.find("versioning")
    assert versioning is not None
    assert versioning.get("type") == "trashcan"
    cleanout = versioning.find('param[@key="cleanoutDays"]')
    assert cleanout is not None
    assert cleanout.get("val") == "100"


def test_spoke_membership_is_only_self_and_hub(tmp_path):
    folder = skills_folder(run_patcher(tmp_path, "spoke-node-a"))
    assert {device.get("id") for device in folder.findall("device")} == {
        "SPOKE-2-ID",
        "HUB-ID",
    }


def test_setup_creates_managed_skills_path_and_marker():
    source = SETUP.read_text(encoding="utf-8")
    assert (
        'BLUEPRINTS_SKILLS_DIR="${BLUEPRINTS_SKILLS_DIR:-/xarta-node/.lone-wolf/skills}"' in source
    )
    assert 'touch "$SKILLS_DIR/.stfolder"' in source
    assert 'chown_like "$SKILLS_DIR" "$SKILLS_DIR/.stfolder"' in source
    assert 'SYNCTHING_SKILLS_HUB_NODE_ID="${SYNCTHING_SKILLS_HUB_NODE_ID:' in source
