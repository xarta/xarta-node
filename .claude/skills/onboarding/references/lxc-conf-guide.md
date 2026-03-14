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
| `hostname:` | `BLUEPRINTS_NODE_ID`, `TAILSCALE_HOSTNAME` | Both get the same value |
| `net0: ... ip=<ADMIN-VLAN-IP>/24` | Admin VLAN IP — use for SSH if Tailscale is unreachable | Always the management network interface |
| `<WAN-B-interface>: ... ip=<WAN-B-IP>/24` | `GW_SECONDARY` → that subnet's gateway (e.g. x.x.x.254) | Whichever netN is WAN-B on this LXC |
| `<WAN-A-interface>: ... ip=<WAN-A-IP>/24` | `GW_PRIMARY` → that subnet's gateway (e.g. x.x.x.254) | Whichever netN is WAN-A on this LXC |
| WAN-B interface name | `IF_SECONDARY="<netN>"` | Read from LXC conf |
| WAN-A interface name | `IF_PRIMARY="<netN>"` | Read from LXC conf |
| All net interfaces (IPs, strip host part) | `TAILSCALE_ROUTES` | Collect each /24 network |

## Deriving TAILSCALE_ROUTES

Take every `ip=<address>/<prefix>` across all net interfaces and zero the host part:
```
net0: <A.B.C.D>/24  → <A.B.C.0>/24
net1: <E.F.G.H>/24  → <E.F.G.0>/24
... one entry per interface
```
Comma-join all results: `TAILSCALE_ROUTES=<A.B.C.0>/24,<E.F.G.0>/24,...`

## Deriving TAILSCALE_IP (informational)

After `tailscale up` has been run on the new node, its Tailscale IP can be retrieved with:
```bash
ssh ... root@<TARGET> "tailscale ip -4"
```
This is useful for verification but is not a standalone `.env` variable — fleet routing
is handled via `NODES_JSON_PATH` and the `.nodes.json` single source of truth.

## Deriving CADDY_EXTRA_NAMES

Any additional DNS names that should serve HTTPS on this node, e.g.:
`<hostname>.<your-internal-domain>`
