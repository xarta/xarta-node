---
name: onboarding
description: Onboard a new xarta-node LXC into the Blueprints fleet. Use when the user wants to add a new node, set up a new LXC, or bring a new machine into the Blueprints sync network. May be given a Proxmox LXC config file to work from.
---

# Onboarding a new Blueprints node

## What you need first

Ask for (or locate) the **Proxmox LXC config** for the target container. Read
[references/lxc-conf-guide.md](references/lxc-conf-guide.md) to extract:
- Admin VLAN IP (the always-reachable local management IP from `eth0`)
- Tailscale IP (if the node is already on the tailnet)
- `hostname` → becomes NODE_ID, NODE_NAME, TAILSCALE_HOSTNAME, CADDY_EXTRA_NAMES subdomain
- WAN interface names → GW_SECONDARY / GW_PRIMARY

## Connectivity: which IP to use for SSH

| Situation | Use |
|-----------|-----|
| New node already on the **same tailnet** as the fleet | Tailscale IP |
| New node on a **different tailnet**, or not yet on Tailscale | Admin VLAN IP — existing fleet nodes advertise the admin VLAN subnet into their tailnet, so it is always routable from any fleet node |

Try Tailscale first:
```bash
ssh -i /root/.ssh/<deploy-key> root@<TS-IP>
```
If that fails, fall back to the admin VLAN IP:
```bash
ssh -i /root/.ssh/<deploy-key> root@<ADMIN-VLAN-IP>
```
If neither works the deploy SSH key may not be installed on the target yet — see
[references/gotchas.md § SSH key not present](references/gotchas.md).

---

## Onboarding checklist

Work through these in order. All commands run **from an existing fleet node** unless noted.

### 0. Pre-flight: confirm SSH access
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> "echo ok"
```

### 1. Install git and python3-venv (often missing on fresh LXCs)
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "apt-get install -y git python3.11-venv 2>&1 | tail -5"
```

### 2. Bootstrap the deploy SSH key on the target
```bash
scp -i /root/.ssh/<deploy-key> \
  /root/.ssh/<deploy-key> \
  /root/.ssh/<deploy-key>.pub \
  root@<TARGET-IP>:/root/.ssh/
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "chmod 600 /root/.ssh/<deploy-key> && \
   chmod 644 /root/.ssh/<deploy-key>.pub && \
   ssh-keyscan github.com >> /root/.ssh/known_hosts 2>/dev/null && \
   ssh -T -i /root/.ssh/<deploy-key> git@github.com 2>&1 | grep -i 'successfully\|denied'"
```

### 3. Clone repos
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> "
  cd /root && git clone git@github.com:<your-org>/xarta-node.git xarta-node
  cd xarta-node
  GIT_SSH_COMMAND='ssh -i /root/.ssh/<deploy-key>' \
    git clone git@github.com:<your-account>/xarta-node-private.git .xarta
"
```

### 4. Copy private files (.secrets/ and .certs/ are gitignored — NOT in the cloned repo)
```bash
scp -r -i /root/.ssh/<deploy-key> \
  /root/xarta-node/.xarta/.secrets/ root@<TARGET-IP>:/root/xarta-node/.xarta/
scp -r -i /root/.ssh/<deploy-key> \
  /root/xarta-node/.xarta/.certs/   root@<TARGET-IP>:/root/xarta-node/.xarta/
```

### 5. Write .env
See [references/env-template.md](references/env-template.md) for the full template with
all values to substitute. Write it with:
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "cat > /root/xarta-node/.env << 'ENVEOF'
<paste filled-in template here>
ENVEOF"
```
Verify: `ssh ... "grep BLUEPRINTS_NODE_ID /root/xarta-node/.env"`

### 6. Setup hosts file (fleet DNS)
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "cd /root/xarta-node && bash setup-hosts.sh"
```
> `fleet-hosts.conf` lives in `.xarta/` (private repo) — already present after step 3.
> Tailscale tagged devices do not receive MagicDNS; this script writes a static `/etc/hosts`
> block so fleet hostnames resolve on every node. See [references/gotchas.md](references/gotchas.md).

### 7. Run setup scripts in order
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> "
  cd /root/xarta-node
  bash setup-ssh-and-git.sh
  bash setup-certificates.sh   # uses supplied certs from .certs/; falls back to self-signed
  bash setup-blueprints.sh
  bash setup-tailscale-up.sh
  bash setup-caddy.sh
"
```

### 8. Verify
```bash
# From an existing fleet node (hostname resolves via /etc/hosts set in step 6):
curl -sk --cacert /root/xarta-node/.xarta/.certs/<your-ca>.crt \
  https://<NEW-NODE>.<your-tailnet>.ts.net/health | python3 -m json.tool
# Expect: "status": "ok", "node_id": "<NEW-NODE>", "integrity_ok": true
# gen advances automatically via boot-catchup from BLUEPRINTS_PEERS
```

### 9. Register in the fleet
See [references/fleet-registry.md](references/fleet-registry.md) for the pattern.
Register the new node on every existing node, and register every existing node on the new one.

### 10. Update BLUEPRINTS_PEERS on all nodes
Every node's `.env` BLUEPRINTS_PEERS must include the new node's address.
Edit each node's `.env` and `systemctl restart blueprints-app`.

### 11. Update fleet-hosts.conf on all nodes
Add the new node's IP/hostname to `.xarta/fleet-hosts.conf`, commit + push to
the private repo, then run `setup-hosts.sh` on every node.

### 12. Sync env backup
```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "cd /root/xarta-node && bash .xarta/sync-env-from-xarta-node.sh"
# Then commit + push .xarta/ on the new node
```

---

## Reference files

| File | When to read |
|------|-------------|
| [references/lxc-conf-guide.md](references/lxc-conf-guide.md) | Parsing a Proxmox LXC conf → extracting IPs, hostnames, interface names |
| [references/env-template.md](references/env-template.md) | Full .env template with every value and substitution guide |
| [references/fleet-registry.md](references/fleet-registry.md) | How to register a new node across all existing nodes |
| [references/gotchas.md](references/gotchas.md) | Known failure modes and fixes from real onboardings |
