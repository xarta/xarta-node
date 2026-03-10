# Onboarding Gotchas

Failure modes encountered in real onboardings — check here before assuming something is broken.

---

## SSH key not present on target

**Symptom:** `Permission denied (publickey)` when SSH-ing to the new node.

**Cause:** `setup-lxc-failover.sh` installs the fleet authorized key. If the LXC was
created without running that script, the deploy key won't be accepted.

**Fix:** From the Proxmox host, exec into the container and install the key manually:
```bash
# On the Proxmox host:
pct exec <LXC-ID> -- bash -c "
  mkdir -p /root/.ssh && chmod 700 /root/.ssh
  echo '<contents of your deploy key .pub file>' >> /root/.ssh/authorized_keys
  chmod 600 /root/.ssh/authorized_keys
"
```
Then retry SSH from the existing fleet node.

---

## git and python3-venv not installed on fresh LXC

**Symptom:** `bash: git: command not found`  
**Or:** `setup-blueprints.sh` fails with: *"The virtual environment was not created
successfully because ensurepip is not available"*

**Fix:** Install both at the same time in step 1:
```bash
apt-get install -y git python3.11-venv
```
If you forget `python3.11-venv`, blueprints setup will fail even after git is working.

---

## Wrong private repo URL

**Symptom:** `ERROR: Repository not found` when cloning `.xarta`

**Cause:** The private repo may be under a personal GitHub account rather than the org.

**Fix:** Check the correct remote URL from an existing node:
```bash
git -C /root/xarta-node/.xarta remote get-url origin
```
Use that exact URL in the clone command; don't assume it matches the public repo's org.

---

## .certs/ and .secrets/ are gitignored — not present after cloning

**Symptom:** After cloning the private repo, `.xarta/.certs/` and `.xarta/.secrets/`
are empty or absent. `setup-certificates.sh` falls back to generating a self-signed cert,
causing browser certificate warnings.

**Cause:** Both directories are in `.xarta/.gitignore` and are never committed.

**Fix:** Always copy them explicitly from an existing node **before** running setup scripts:
```bash
scp -r -i /root/.ssh/<deploy-key> \
  /root/xarta-node/.xarta/.secrets/ root@<TARGET-IP>:/root/xarta-node/.xarta/
scp -r -i /root/.ssh/<deploy-key> \
  /root/xarta-node/.xarta/.certs/   root@<TARGET-IP>:/root/xarta-node/.xarta/
```

If you discover this after Caddy is already up with a self-signed cert:
```bash
# After copying certs, update .env cert paths and re-run Caddy setup:
ssh ... root@<TARGET-IP> "
  sed -i \
    -e 's|CERT_FILE=.*|CERT_FILE=/root/xarta-node/.xarta/.certs/<your-cert>.crt|' \
    -e 's|CERT_KEY=.*|CERT_KEY=/root/xarta-node/.xarta/.certs/<your-cert>.key|' \
    -e 's|CERT_CA=.*|CERT_CA=/root/xarta-node/.xarta/.certs/<your-ca>.crt|' \
    /root/xarta-node/.env
  cd /root/xarta-node && bash setup-caddy.sh
"
```

---

## MagicDNS doesn't resolve for tagged Tailscale devices

**Symptom:** `curl: (6) Could not resolve host: <node>.<your-tailnet>.ts.net`

**Cause:** Tailscale tagged devices do not receive MagicDNS. The `*.<tailnet>.ts.net`
hostnames are only automatically resolvable by user-owned (non-tagged) devices.

**Fix:** `setup-hosts.sh` writes a static `/etc/hosts` block from `fleet-hosts.conf`
(in the private repo). Run it on every node:
```bash
bash /root/xarta-node/setup-hosts.sh
```
When adding a new node, update `fleet-hosts.conf` in the private repo and run
`setup-hosts.sh` on **all** existing nodes too.

---

## Caddy health check reports HTTP 000 from inside the container

**Symptom:** `setup-caddy.sh` step 8 reports `HTTP 000` for the HTTPS hostname.

**Cause:** The self-check inside the script resolves the hostname via system DNS, which
suffers from the tagged-device MagicDNS problem above (the hosts block is written by
`setup-hosts.sh` which runs before `setup-caddy.sh`, but the self-check inside Caddy's
setup script resolves from within the container against its own `/etc/hosts` — if that
wasn't populated before Caddy ran it will fail).

**Confirm the node is actually up** by testing from an existing fleet node instead:
```bash
curl -sk --cacert /root/xarta-node/.xarta/.certs/<your-ca>.crt \
  https://<NEW-NODE>.<your-tailnet>.ts.net/health
```

---

## gen stays at 0 after setup-blueprints.sh succeeds

**Symptom:** `/health` returns `"gen": 0` immediately after setup.

**Cause:** Boot-catchup from `BLUEPRINTS_PEERS` is asynchronous — wait ~10 seconds.

If it stays at 0, check that the peer is reachable:
```bash
curl -s http://<PEER-TS-IP>:8080/health
```
Then check service logs:
```bash
journalctl -u blueprints-app -n 50 --no-pager
```
Common cause: `BLUEPRINTS_PEERS` IP is wrong or the peer's port 8080 isn't reachable
(firewall, wrong interface, peer service not running).
