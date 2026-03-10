# LXC Config Guide — Extracting Onboarding Values

Given a Proxmox LXC config block, extract these values before writing `.env`.

## Example config (fictional)

```
hostname: alpha-node
net0: name=eth0,bridge=vmbr0,firewall=1,hwaddr=...,ip=<ADMIN-VLAN-IP>/24,type=veth
net1: name=net1,bridge=vmbr1,hwaddr=...,ip=<VLAN-B-IP>/24,type=veth
net2: name=net2,bridge=vmbr0,hwaddr=...,ip=<VLAN-C-IP>/24,tag=3,type=veth
net3: name=net3,bridge=vmbr1,ip=<WAN-B-IP>/24,tag=20,type=veth
net4: name=net4,bridge=vmbr1,ip=<WAN-A-IP>/24,tag=22,type=veth
```

## Field mapping

| LXC conf field | Extracts to | Notes |
|----------------|-------------|-------|
| `hostname:` | `BLUEPRINTS_NODE_ID`, `BLUEPRINTS_NODE_NAME`, `BLUEPRINTS_HOST_MACHINE`, `TAILSCALE_HOSTNAME` | All four get the same value |
| `net0: ... ip=<ADMIN-VLAN-IP>/24` | Admin VLAN IP — use for SSH if Tailscale is unreachable | Always the management network interface |
| `net3: ... ip=<WAN-B-IP>/24` | `GW_SECONDARY` → that subnet's gateway (e.g. x.x.x.254) | net3 = IF_SECONDARY |
| `net4: ... ip=<WAN-A-IP>/24` | `GW_PRIMARY` → that subnet's gateway (e.g. x.x.x.254) | net4 = IF_PRIMARY |
| net3 interface name | `IF_SECONDARY="net3"` | |
| net4 interface name | `IF_PRIMARY="net4"` | |
| All net interfaces (IPs, strip host part) | `TAILSCALE_ROUTES` | Collect each /24 network |

## Deriving TAILSCALE_ROUTES

Take every `ip=<address>/<prefix>` across all net interfaces and zero the host part:
```
net0: <A.B.C.D>/24  → <A.B.C.0>/24
net1: <E.F.G.H>/24  → <E.F.G.0>/24
... one entry per interface
```
Comma-join all results: `TAILSCALE_ROUTES=<A.B.C.0>/24,<E.F.G.0>/24,...`

## Deriving BLUEPRINTS_SELF_ADDRESS

This is the Tailscale IP of the new node, port 8080.
Find it after `tailscale up` has been run:
```bash
ssh ... root@<TARGET> "tailscale ip -4"
```
Or check `tailscale status` on any existing fleet node after the new one joins.

## Deriving BLUEPRINTS_UI_URL

`https://<hostname>.<your-tailnet>.ts.net`

## Deriving CADDY_EXTRA_NAMES

Any additional DNS names that should serve HTTPS on this node, e.g.:
`<hostname>.<your-internal-domain>`

## Deriving BLUEPRINTS_CORS_ORIGINS

Include ALL current fleet nodes (both HTTPS names per node if applicable):
```
https://existing-node-1.<your-tailnet>.ts.net,
https://existing-node-1.<your-internal-domain>,
https://existing-node-2.<your-tailnet>.ts.net,
...
https://<NEW-NODE>.<your-tailnet>.ts.net,
https://<NEW-NODE>.<your-internal-domain>
```
No trailing slash, comma-separated, no line breaks in the actual `.env` value.
