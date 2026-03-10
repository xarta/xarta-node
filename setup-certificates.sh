#!/bin/bash

# setup-certificates.sh
# Sets up TLS certificates for this node.
#
# Behaviour (idempotent):
#   1. Reads CERTS_DIR from .env (defaults to $REPO_INNER_PATH/.certs).
#   2. Creates the directory if it doesn't exist.
#   3. If CERT_FILE and CERT_KEY are set and both files exist:
#        - Installs CERT_CA into the system trust store (if set and not yet installed).
#        - Reports details and exits.
#   4. Otherwise scans CERTS_DIR for usable cert/key pairs:
#        - Skips CA certificates (those that have CA:TRUE in their X.509 extension).
#        - If one pair found — selects it.
#        - If multiple pairs found — prefers one with "wildcard" in the name.
#        - Installs any found CA cert into the system trust store.
#   5. If no usable pair is found — generates a CA-signed certificate:
#        a. Looks for an existing CA cert+key pair in CERTS_DIR (one that can sign).
#        b. If none found — generates a local CA (local-ca.crt / local-ca.key).
#        c. Generates a leaf cert signed by that CA (not self-signed).
#        d. Installs the CA into the system trust store so clients that import the
#           CA cert will get a trusted connection without browser warnings.
#   6. Updates CERTS_DIR / CERT_FILE / CERT_KEY / CERT_CA in .env.
#
# The installed CA cert is at /usr/local/share/ca-certificates/<name>.crt.
# Export it and import it into client browsers / OS trust stores for testing.
#
# Called standalone or from setup-caddy.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Colours ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── Load .env ──────────────────────────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env not found at $ENV_FILE" >&2
    exit 1
fi
source "$ENV_FILE"

# ── Helpers ────────────────────────────────────────────────────────────────────

# Add or update a key=value line in .env (no quotes added — matches our .env style).
env_set() {
    local key="$1" value="$2"
    if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        echo -e "    ${CYAN}updated${NC}: ${key}=${value}"
    else
        echo "" >> "$ENV_FILE"
        echo "${key}=${value}" >> "$ENV_FILE"
        echo -e "    ${CYAN}added${NC}:   ${key}=${value}"
    fi
}

# Return 0 if the cert at $1 is a CA certificate (CA:TRUE in basic constraints).
is_ca_cert() {
    openssl x509 -noout -text -in "$1" 2>/dev/null \
        | grep -q "CA:TRUE"
}

# Extract hostname (no scheme, no port) from a URL.
url_host() {
    echo "$1" | sed 's|^https\?://||' | sed 's|:.*||' | sed 's|/.*||'
}

# Extract IP address from a URL, or empty string if it's a hostname.
url_ip() {
    local host
    host=$(url_host "$1")
    if [[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "$host"
    fi
}

# Install a CA certificate into the system trust store (Debian/Ubuntu).
# Idempotent — skips if the identical file is already installed.
install_ca() {
    local ca_cert="$1"
    local ca_name
    ca_name="$(basename "${ca_cert%.crt}")"
    local dest="/usr/local/share/ca-certificates/${ca_name}.crt"

    if [[ -f "$dest" ]] && diff -q "$ca_cert" "$dest" >/dev/null 2>&1; then
        echo -e "    CA already in system trust store: ${dest}"
        return 0
    fi

    echo "Installing CA into system trust store..."
    cp "$ca_cert" "$dest"
    update-ca-certificates 2>&1 | grep -E '^[0-9]|added|error' | sed 's/^/    /'
    echo -e "    ${GREEN}CA trusted by system${NC}: ${dest}"
    echo -e "    ${YELLOW}Tip:${NC} Export ${ca_cert} and import it into client browsers / OS trust stores."
}

# Find a CA cert+key pair in CERTS_DIR that can be used for signing.
# Sets CA_SIGN_CERT and CA_SIGN_KEY globals. Returns 0 if found, 1 if not.
CA_SIGN_CERT=""
CA_SIGN_KEY=""
find_ca_keypair() {
    CA_SIGN_CERT="" CA_SIGN_KEY=""
    while IFS= read -r crtfile; do
        is_ca_cert "$crtfile" || continue
        local keyfile="${crtfile%.crt}.key"
        [[ -f "$keyfile" ]] || continue
        CA_SIGN_CERT="$crtfile"
        CA_SIGN_KEY="$keyfile"
        return 0
    done < <(find "$CERTS_DIR" -maxdepth 1 -name "*.crt" | sort)
    return 1
}

# ── Determine and provision CERTS_DIR ─────────────────────────────────────────
echo "=== Certificate setup ==="
echo ""

if [[ -z "${CERTS_DIR:-}" ]]; then
    if [[ -z "${REPO_INNER_PATH:-}" ]]; then
        echo "Error: CERTS_DIR and REPO_INNER_PATH are both unset in .env." >&2
        echo "Set CERTS_DIR explicitly and re-run." >&2
        exit 1
    fi
    CERTS_DIR="${REPO_INNER_PATH}/.certs"
    echo "CERTS_DIR not set — defaulting to ${CERTS_DIR}"
    env_set "CERTS_DIR" "$CERTS_DIR"
fi

if [[ ! -d "$CERTS_DIR" ]]; then
    echo "Creating certificate directory: $CERTS_DIR"
    mkdir -p "$CERTS_DIR"
    chmod 700 "$CERTS_DIR"
    echo -e "    ${GREEN}ok${NC}"
else
    echo "Certificate directory: $CERTS_DIR"
fi
echo ""

# ── Check if already configured and files exist ───────────────────────────────
if [[ -n "${CERT_FILE:-}" && -n "${CERT_KEY:-}" ]]; then
    if [[ -f "$CERT_FILE" && -f "$CERT_KEY" ]]; then
        echo -e "${GREEN}Certificates already configured:${NC}"
        echo "    CERT_FILE = $CERT_FILE"
        echo "    CERT_KEY  = $CERT_KEY"
        [[ -n "${CERT_CA:-}" && -f "${CERT_CA}" ]] && echo "    CERT_CA   = $CERT_CA"
        echo ""
        # Report cert info
        echo "Certificate details:"
        openssl x509 -noout -subject -issuer -dates -in "$CERT_FILE" 2>/dev/null \
            | sed 's/^/    /'
        echo ""
        echo "SANs:"
        openssl x509 -noout -ext subjectAltName -in "$CERT_FILE" 2>/dev/null \
            | sed 's/^/    /'
        echo ""
        # Ensure CA is installed in system trust store even if cert hasn't changed.
        if [[ -n "${CERT_CA:-}" && -f "${CERT_CA}" ]]; then
            install_ca "$CERT_CA"
            echo ""
        fi
        echo "No changes needed."
        exit 0
    else
        echo -e "${YELLOW}Warning:${NC} CERT_FILE/CERT_KEY set in .env but file(s) missing — rescanning."
        echo ""
    fi
fi

# ── Scan for existing cert/key pairs in CERTS_DIR ─────────────────────────────
echo "Scanning $CERTS_DIR for usable cert/key pairs..."
echo ""

selected_cert=""
selected_key=""
selected_ca=""

declare -a pairs=()

# Find all .key files with a matching .crt (same basename, not a CA cert).
while IFS= read -r keyfile; do
    base="${keyfile%.key}"
    crtfile="${base}.crt"
    [[ -f "$crtfile" ]] || continue
    is_ca_cert "$crtfile" && continue   # skip CA certs
    pairs+=("$base")
done < <(find "$CERTS_DIR" -maxdepth 1 -name "*.key" | sort)

# Find any CA cert (has CA:TRUE, has a .crt, no .key partner needed).
while IFS= read -r crtfile; do
    is_ca_cert "$crtfile" && selected_ca="$crtfile" && break
done < <(find "$CERTS_DIR" -maxdepth 1 -name "*.crt" | sort)

if [[ "${#pairs[@]}" -gt 1 ]]; then
    # Prefer a cert with "wildcard" in the name; otherwise take the first.
    for base in "${pairs[@]}"; do
        if [[ "$base" == *wildcard* || "$base" == *multi* ]]; then
            selected_cert="${base}.crt"
            selected_key="${base}.key"
            break
        fi
    done
    if [[ -z "$selected_cert" ]]; then
        selected_cert="${pairs[0]}.crt"
        selected_key="${pairs[0]}.key"
    fi
    echo "Multiple cert/key pairs found — selected:"
elif [[ "${#pairs[@]}" -eq 1 ]]; then
    selected_cert="${pairs[0]}.crt"
    selected_key="${pairs[0]}.key"
    echo "Found cert/key pair:"
fi

# ── Use found pair ─────────────────────────────────────────────────────────────
if [[ -n "$selected_cert" ]]; then
    echo "    cert: $selected_cert"
    echo "    key:  $selected_key"
    [[ -n "$selected_ca" ]] && echo "    CA:   $selected_ca"
    echo ""
    echo "Certificate details:"
    openssl x509 -noout -subject -issuer -dates -in "$selected_cert" 2>/dev/null \
        | sed 's/^/    /'
    echo ""
    echo "SANs:"
    openssl x509 -noout -ext subjectAltName -in "$selected_cert" 2>/dev/null \
        | sed 's/^/    /'
    echo ""
    echo "Updating .env..."
    env_set "CERT_FILE" "$selected_cert"
    env_set "CERT_KEY"  "$selected_key"
    if [[ -n "$selected_ca" ]]; then
        env_set "CERT_CA" "$selected_ca"
        echo ""
        install_ca "$selected_ca"
    fi
    echo ""
    echo -e "${GREEN}Done.${NC}"
    exit 0
fi

# ── No certs found — generate CA + CA-signed leaf cert ────────────────────────
echo "No usable cert/key pairs found — generating certificates."
echo ""

OPENSSL_TMPDIR="$(mktemp -d /tmp/openssl-certs.XXXXXX)"
trap 'rm -rf "$OPENSSL_TMPDIR"' EXIT

# ── Build SAN list from known node URLs in .env ────────────────────────────────
declare -a SANS_DNS=("localhost")
declare -a SANS_IP=("127.0.0.1")

for url_var in BLUEPRINTS_UI_URL BLUEPRINTS_SELF_ADDRESS; do
    url="${!url_var:-}"
    [[ -z "$url" ]] && continue
    host=$(url_host "$url")
    ip=$(url_ip "$url")
    if [[ -n "$ip" ]]; then
        SANS_IP+=("$ip")
    elif [[ -n "$host" && "$host" != "localhost" ]]; then
        SANS_DNS+=("$host")
    fi
done

mapfile -t SANS_DNS < <(printf '%s\n' "${SANS_DNS[@]}" | sort -u)
mapfile -t SANS_IP  < <(printf '%s\n' "${SANS_IP[@]}"  | sort -u)

SAN_STRING=""
for dns in "${SANS_DNS[@]}"; do SAN_STRING+="DNS:${dns},"; done
for ip  in "${SANS_IP[@]}";  do SAN_STRING+="IP:${ip},";   done
SAN_STRING="${SAN_STRING%,}"

echo "SANs to include in leaf cert:"
echo "    DNS: ${SANS_DNS[*]}"
echo "    IP:  ${SANS_IP[*]}"
echo ""

# ── Find or generate a signing CA ─────────────────────────────────────────────
if find_ca_keypair; then
    echo -e "Found signable CA pair:"
    echo "    cert: $CA_SIGN_CERT"
    echo "    key:  $CA_SIGN_KEY"
    echo ""
    NEW_CA_CERT=""
else
    NEW_CA_CERT="$CERTS_DIR/local-ca.crt"
    NEW_CA_KEY="$CERTS_DIR/local-ca.key"
    CA_CNF="$OPENSSL_TMPDIR/ca.cnf"

    cat > "$CA_CNF" <<EOCACNF
[req]
default_bits       = 4096
prompt             = no
default_md         = sha256
distinguished_name = dn
x509_extensions    = v3_ca

[dn]
C  = GB
ST = Unknown
L  = Unknown
O  = Xarta Local CA
CN = Xarta Local CA

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage         = critical,keyCertSign,cRLSign
EOCACNF

    echo "Generating local CA (4096-bit RSA, 10-year validity)..."
    openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 \
        -keyout "$NEW_CA_KEY" \
        -out    "$NEW_CA_CERT" \
        -config "$CA_CNF" \
        -nodes 2>/dev/null
    chmod 600 "$NEW_CA_KEY"
    echo -e "    ${GREEN}ok${NC} — ${NEW_CA_CERT}"
    echo ""

    CA_SIGN_CERT="$NEW_CA_CERT"
    CA_SIGN_KEY="$NEW_CA_KEY"
fi

# ── Generate leaf key + CSR ────────────────────────────────────────────────────
NEW_CERT="$CERTS_DIR/local-node.crt"
NEW_KEY="$CERTS_DIR/local-node.key"
LEAF_CSR="$OPENSSL_TMPDIR/leaf.csr"
LEAF_CNF="$OPENSSL_TMPDIR/leaf.cnf"
LEAF_EXT="$OPENSSL_TMPDIR/leaf-ext.cnf"

cat > "$LEAF_CNF" <<EOLEAFCNF
[req]
default_bits       = 4096
prompt             = no
default_md         = sha256
distinguished_name = dn

[dn]
C  = GB
ST = Unknown
L  = Unknown
O  = Xarta Node
CN = localhost
EOLEAFCNF

cat > "$LEAF_EXT" <<EOEXT
[v3_req]
subjectAltName   = ${SAN_STRING}
basicConstraints = CA:FALSE
keyUsage         = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOEXT

echo "Generating leaf key and CSR..."
openssl req -new -newkey rsa:4096 \
    -keyout "$NEW_KEY" \
    -out    "$LEAF_CSR" \
    -config "$LEAF_CNF" \
    -nodes 2>/dev/null
chmod 600 "$NEW_KEY"
echo -e "    ${GREEN}ok${NC}"

# ── Sign leaf cert with CA ─────────────────────────────────────────────────────
echo "Signing leaf certificate with CA..."
openssl x509 -req \
    -in      "$LEAF_CSR" \
    -CA      "$CA_SIGN_CERT" \
    -CAkey   "$CA_SIGN_KEY" \
    -CAcreateserial \
    -out     "$NEW_CERT" \
    -days    3650 \
    -sha256 \
    -extfile "$LEAF_EXT" \
    -extensions v3_req 2>/dev/null
echo -e "    ${GREEN}ok${NC}"
echo ""

echo "Certificate details:"
openssl x509 -noout -subject -issuer -dates -in "$NEW_CERT" 2>/dev/null \
    | sed 's/^/    /'
echo ""
echo "SANs:"
openssl x509 -noout -ext subjectAltName -in "$NEW_CERT" 2>/dev/null \
    | sed 's/^/    /'
echo ""

# ── Install CA into system trust store ────────────────────────────────────────
install_ca "$CA_SIGN_CERT"
echo ""

# ── Update .env ───────────────────────────────────────────────────────────────
echo "Updating .env..."
env_set "CERT_FILE" "$NEW_CERT"
env_set "CERT_KEY"  "$NEW_KEY"
env_set "CERT_CA"   "$CA_SIGN_CERT"
echo ""

echo -e "${GREEN}Done.${NC}"
echo ""
echo -e "${YELLOW}Tip:${NC} Import the CA cert into your client browser / OS trust store:"
echo "      ${CA_SIGN_CERT}"
echo "     Clients that trust this CA will get a valid certificate without warnings."
