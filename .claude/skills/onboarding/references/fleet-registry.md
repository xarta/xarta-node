# Fleet Registry

This file describes the fleet registration pattern. Actual node IPs and names
are stored in the **private repo** (`.xarta/`) — see the onboarding skill there.

## What "registered" means

Each Blueprints node maintains its own list of peers in a local SQLite DB.
Nodes are not auto-discovered — each must be explicitly told about every other node
via the REST API. The `BLUEPRINTS_PEERS` `.env` var provides the bootstrap peer
for initial DB sync, but full mesh registration must be done manually after onboarding.

## Registering a new node

For each existing node in the fleet, POST the new node's details to its API:

```bash
curl -s -X POST http://<EXISTING-NODE-TS-IP>:8080/api/v1/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "node_id":      "<NEW-NODE-HOSTNAME>",
    "display_name": "<NEW-NODE-HOSTNAME>",
    "host_machine": "<NEW-NODE-HOSTNAME>",
    "addresses":    ["http://<NEW-NODE-TS-IP>:8080"],
    "ui_url":       "https://<NEW-NODE-HOSTNAME>.<your-tailnet>.ts.net"
  }'
```

Also register every existing node on the new node:

```bash
curl -s -X POST http://<NEW-NODE-TS-IP>:8080/api/v1/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "node_id":      "<EXISTING-NODE-HOSTNAME>",
    "display_name": "<EXISTING-NODE-HOSTNAME>",
    "host_machine": "<EXISTING-NODE-HOSTNAME>",
    "addresses":    ["http://<EXISTING-NODE-TS-IP>:8080"],
    "ui_url":       "https://<EXISTING-NODE-HOSTNAME>.<your-tailnet>.ts.net"
  }'
```

## Scripting registration across the whole fleet

```bash
NEW_NODE_ID="<new-node-hostname>"
NEW_TS_IP="<new-node-tailscale-ip>"
PAYLOAD="{\"node_id\":\"$NEW_NODE_ID\",\"display_name\":\"$NEW_NODE_ID\",\
\"host_machine\":\"$NEW_NODE_ID\",\"addresses\":[\"http://$NEW_TS_IP:8080\"],\
\"ui_url\":\"https://$NEW_NODE_ID.<your-tailnet>.ts.net\"}"

# Register new node on all existing nodes:
for IP in <node-1-ts-ip> <node-2-ts-ip> <node-3-ts-ip>; do
  curl -s -X POST http://$IP:8080/api/v1/nodes \
    -H 'Content-Type: application/json' \
    -d "$PAYLOAD" | python3 -m json.tool
done

# Register all existing nodes on the new node:
# Build one JSON payload per existing node and POST each to http://$NEW_TS_IP:8080/api/v1/nodes
```

## Updating BLUEPRINTS_PEERS after adding a node

On every node in the fleet, update `.env` and restart:

```bash
# Edit BLUEPRINTS_PEERS to be a comma-separated list of all node HTTP addresses:
sed -i 's|BLUEPRINTS_PEERS=.*|BLUEPRINTS_PEERS=http://<node-1-ts-ip>:8080,http://<node-2-ts-ip>:8080,...|' \
  /root/xarta-node/.env
systemctl restart blueprints-app
```

Do this on the **new node too** — it needs to know about all peers, not just the bootstrap node.

## Checking fleet health

```bash
for NODE in <node-1> <node-2> <node-3>; do
  echo "=== $NODE ==="
  curl -sk --cacert /root/xarta-node/.xarta/.certs/<your-ca>.crt \
    https://$NODE.<your-tailnet>.ts.net/health | python3 -m json.tool
done
```

All `gen` values should match within a few seconds of each other (boot-catchup is asynchronous).

## Note on boot-catchup

When a new node starts with `BLUEPRINTS_PEERS` pointing to an existing node, it
automatically requests a full DB copy (`gen=0` → fleet gen) in the background. This
typically completes within a few seconds. If gen stays at 0, check connectivity to
the peer and the blueprints-app service logs:
```bash
journalctl -u blueprints-app -n 50 --no-pager
```
