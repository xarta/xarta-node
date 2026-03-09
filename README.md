# xarta-node

> ⚠️ **AI-GENERATED CODE — DO NOT USE IN PRODUCTION WITHOUT REVIEW**
>
> This repository was generated with AI assistance. It has **not** been
> independently audited or tested in a real environment. Do not deploy
> to production systems without thorough review by a qualified engineer.

---

## Overview

Scripts and configuration for setting up an LXC container with dual-WAN
gateway failover and Tailscale exit-node functionality.

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

## Configuration

All site-specific values (interface names, gateway IPs) live in `.env`,
which is gitignored and must never be committed. See `.env.example` for
the required variables.

## License

[MIT](LICENSE) — see `LICENSE` for details.
