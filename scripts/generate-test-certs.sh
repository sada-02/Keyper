#!/bin/bash
# Script to generate test TLS certificates for Feature F testing

set -e

CERT_DIR="./test-certs"
mkdir -p "$CERT_DIR"

echo "Generating CA certificate..."
openssl genrsa -out "$CERT_DIR/ca-key.pem" 2048
openssl req -new -x509 -days 365 -key "$CERT_DIR/ca-key.pem" \
    -out "$CERT_DIR/ca-cert.pem" \
    -subj "/CN=Keyper Test CA"

echo "Generating server certificate..."
openssl genrsa -out "$CERT_DIR/server-key.pem" 2048
openssl req -new -key "$CERT_DIR/server-key.pem" \
    -out "$CERT_DIR/server.csr" \
    -subj "/CN=localhost"

openssl x509 -req -days 365 \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca-cert.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -out "$CERT_DIR/server-cert.pem" \
    -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")

echo "Generating client certificate..."
openssl genrsa -out "$CERT_DIR/client-key.pem" 2048
openssl req -new -key "$CERT_DIR/client-key.pem" \
    -out "$CERT_DIR/client.csr" \
    -subj "/CN=keyper-client"

openssl x509 -req -days 365 \
    -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca-cert.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" \
    -CAcreateserial \
    -out "$CERT_DIR/client-cert.pem"

# Cleanup CSRs
rm "$CERT_DIR"/*.csr

echo "✅ Test certificates generated in $CERT_DIR/"
echo "   CA:     ca-cert.pem / ca-key.pem"
echo "   Server: server-cert.pem / server-key.pem"
echo "   Client: client-cert.pem / client-key.pem"
