#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

CERTS_DIR="${CERTS_DIR:-"$REPO_ROOT/certs"}"
CERT_DAYS="${CERT_DAYS:-365}"
CERT_CN="${CERT_CN:-localhost}"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate local certificates." >&2
  exit 1
fi

mkdir -p "$CERTS_DIR"

CERT_PATH="$CERTS_DIR/ssl.cert"
KEY_PATH="$CERTS_DIR/ssl.key"
TMP_CONF=$(mktemp)

cleanup() {
  rm -f "$TMP_CONF"
}
trap cleanup EXIT INT TERM

cat > "$TMP_CONF" <<EOF
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = $CERT_CN
O = MVZ443 Server
C = CN

[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

echo "Generating local self-signed certificates..."
echo "  out:  $CERTS_DIR"
echo "  days: $CERT_DAYS"
echo "  CN:   $CERT_CN"

openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -sha256 \
  -days "$CERT_DAYS" \
  -keyout "$KEY_PATH" \
  -out "$CERT_PATH" \
  -config "$TMP_CONF" >/dev/null 2>&1

chmod 600 "$KEY_PATH"
chmod 644 "$CERT_PATH"

echo "Generated:"
echo "  $CERT_PATH"
echo "  $KEY_PATH"
