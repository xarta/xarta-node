# xarta-node - Proxmox LXC

> ⚠️ **AI-GENERATED CODE — DO NOT USE IN PRODUCTION WITHOUT REVIEW**
>
> This repository was generated with AI assistance. It has **not** been
> independently audited or tested in a real environment. Do not deploy
> to production systems without thorough review by a qualified engineer.

---

## Overview

Scripts and configuration for setting up an LXC container with dual-WAN
gateway failover and Tailscale exit-node functionality.

Also - a simple SQL Lite database with automatic distribution of updates to peers.
Intended to support things like links to other services in a homelab context.

My home setup comprises two ISP's and their routers since I like to keep my experimentation
mostly separate from my partner's stable internet connection.  Vendor routers connected
to a switch which then trunks them as VLANs.  I used to use a pfsense router for both 
ISP connections more directly but I worry about if something happens to me, what would my
non-technical partner do so I keep some things "standard".

I then have a standard hardware failover router,
but also a high-availabily pfsense pair of routers all using those ISP router vlans 
for failover.  I prioritise one ISP for myself, while my partner is on the ISP's router's
LAN direct.  For my stuff I introduce double NAT which isn't a problem mostly.  
But for reliabiliy and simpler tailscale routing, I'm using LXC's set-up to
connect to the ISP vlans direct with scripting to failover, with a tailscale client
setup to use either and then advertise itself as an exit node onto my vlans.
Obviously ACL's limit access to this exit!  This avoids double-nat, and I can
deploy such LXC's on different hardware with different UPS set-ups etc.  There's no
reason why a 3rd failover ISP vlan couldn't be added e.g. 4G LTE / 5G etc.  And 
I can use different "nodes" with different tailnets for multiple ways of improving
connectivity reliability.  It makes sense to use these LXC's for other key services
that would benefit from that connectivity reliability and LXC distribution.

Actions are distributable via the database FIFO queue including triggering git pull.
This repo assumes there's an "inner repo" for private assets.  
Assuming the main GUI served by Caddy will go in the private inner repo as could reveal 
secrets about the homelab if looking to optimise the GUI to represent homelab structure.

Very basic fallback GUI on /fallback-ui for basic interaction with database.

**WARNING** - THIS REPO DOES NOT FOLLOW MANY SECURE PRACTICES. IT CURRENTLY ASSUMES ROOT
ACCESS AND SERVICES WEAKLY MITIGATED BY BEING BASED ON UNPRIVILEDGED LXC'S BUT IT WOULD
STILL BE WISE TO REFACTOR TO A NON-ROOT INFRASTRUCTURE ESPECIALLY TO PROTECT KEYS ETC.
THAT COULD BE EXPLOITED ELSEWHERE.

In the xarta homelab environment these nodes will work on isolated tailnets / ACL protected, and local VLAN accessibility will be strictly firewalled using the tools Proxmox provides.
Nonetheless using root as we use it in this repo is somewhat risky.

## Assumed Environment

These scripts are developed and tested against the following environment.
Behaviour on other configurations is not guaranteed.

### Software

| Component | Version |
|-----------|---------|
| **Container type** | Proxmox LXC (unprivileged) |
| **OS** | Debian GNU/Linux 12 (bookworm) |
| **Kernel** | 6.8.12-pve (Proxmox host kernel) |
| **Shell** | Bash 5.2 |
| **Python** | 3.11.2 |
| **systemd** | 252 |

The scripts assume they are run as **root** inside the LXC container.

### Proxmox LXC configuration

The container requires specific Proxmox config to support Tailscale and IP
forwarding. Key settings in the `.conf` file on the Proxmox host:

```ini
# Required for Tailscale (TUN device access)
lxc.cgroup2.devices.allow: c 10:200 rwm
lxc.mount.entry: /dev/net dev/net none bind,create=dir

# Required for nested networking / keyctl
features: keyctl=1,nesting=1

# Recommended: start on boot
onboot: 1
```

> Without the `lxc.cgroup2.devices.allow` and `lxc.mount.entry` lines,
> `/dev/net/tun` will not be accessible inside the container and Tailscale
> will fail to start.

### Network interfaces

The expected network topology is multiple interfaces mapped to different
bridges/VLANs on the Proxmox host — one per subnet you want to advertise
through Tailscale. Typical setup:

```ini
net0: name=eth0,bridge=<main-bridge>,ip=<mgmt-ip>/24          # management
net1: name=net1,bridge=<bridge>,ip=<vlan1-ip>/24              # VLAN 1
net2: name=net2,bridge=<bridge>,ip=<vlan2-ip>/24,tag=<vlan>   # VLAN 2 (tagged)
# ... additional VLANs as needed
```

Interface names, bridges, IPs, and VLAN tags are all site-specific and
configured in `.env` — not committed to this repo.

---

## Usage

1. Copy `.env.example` to `.env` and fill in your site-specific values:

   ```bash
   cp .env.example .env
   nano .env
   ```

2. Run the setup script inside the target LXC container:

   ```bash
   chmod +x setup-lxc-failover.sh
   sudo ./setup-lxc-failover.sh
   ```

The script is idempotent — it is safe to run more than once.

3. **Activate Tailscale:**

   `setup-lxc-failover.sh` installs Tailscale but does not run `tailscale up`
   — route configuration is site-specific. Set the `TAILSCALE_*` values in
   `.env`, then run:

   ```bash
   chmod +x setup-tailscale-up.sh
   ./setup-tailscale-up.sh
   ```

   The script builds and runs `tailscale up` from your `.env` values. If this
   is a new node without a pre-auth key, it will print a login URL — open it
   in a browser to authenticate, then re-run the script.

   > `TAILSCALE_ACCEPT_DNS=false` is intentional for gateway nodes — the
   > node uses its own resolvers (set via `nameserver` in the Proxmox LXC
   > config), not ones pushed by the control server.

4. **Approve advertised routes** in your Tailscale/Headscale admin panel.

   After `tailscale up`, routes are pending until approved in the control
   plane (e.g. `https://<your-headscale-host>/web`). The script will remind
   you of this after a successful run.

   > **DNS note:** If your control plane's DNS resolver needs to reach subnets
   > advertised by this node, ensure the resolver's access control list permits
   > those subnets before approving the routes.

## Configuration

All site-specific values (interface names, gateway IPs) live in `.env`,
which is gitignored and must never be committed. See `.env.example` for
the required variables.

## License

[MIT](LICENSE) — see `LICENSE` for details.
