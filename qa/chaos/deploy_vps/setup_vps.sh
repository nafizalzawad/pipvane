#!/usr/bin/env bash
# ==============================================================================
# Pipvane v3: Headless VPS Setup & Deployment Script for 30-Day Soak Testing
# Target OS: Ubuntu 22.04 LTS / 24.04 LTS / Debian 12 / Rocky Linux 9
# ==============================================================================

set -euo pipefail

echo "=============================================================================="
echo " Pipvane v3: 30-Day Telegram Soak & Chaos Test VPS Provisioner"
echo "=============================================================================="

# 1. Check Root Privileges
if [ "$EUID" -ne 0 ]; then
  echo "[-] Error: Please execute as root or with sudo."
  exit 1
fi

INSTALL_DIR="/opt/pipvane"
DATA_DIR="${INSTALL_DIR}/data/soak_telemetry"
SERVICE_NAME="pipvane-telegram-soak"
SERVICE_USER="pipvane"

echo "[+] Updating apt package repositories..."
apt-get update -y
apt-get install -y --no-install-recommends \
  git \
  curl \
  wget \
  build-essential \
  cmake \
  gperf \
  libssl-dev \
  zlib1g-dev \
  python3 \
  python3-pip \
  python3-venv \
  python3-dev \
  tmux \
  htop \
  jq

# 2. Create System User
if ! id "${SERVICE_USER}" &>/dev/null; then
  echo "[+] Creating dedicated system user: ${SERVICE_USER}..."
  useradd -r -m -d /opt/pipvane -s /bin/bash "${SERVICE_USER}"
fi

# 3. Create Directory Structure
echo "[+] Preparing directories in ${INSTALL_DIR}..."
mkdir -p "${INSTALL_DIR}"
mkdir -p "${DATA_DIR}"
mkdir -p "${DATA_DIR}/sessions"

# 4. Clone / Copy Repository
if [ ! -f "${INSTALL_DIR}/qa/chaos/soak_orchestrator.py" ]; then
  echo "[*] Syncing workspace code to ${INSTALL_DIR}..."
  cp -r . "${INSTALL_DIR}/" || true
fi

# 5. Setup Python Virtual Environment
echo "[+] Initializing Python virtual environment..."
python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip
"${INSTALL_DIR}/venv/bin/pip" install psutil pytest pytest-asyncio flake8

# 6. Install Systemd Service
echo "[+] Installing systemd service: ${SERVICE_NAME}.service..."
cp "${INSTALL_DIR}/qa/chaos/deploy_vps/pipvane-telegram-soak.service" "/etc/systemd/system/${SERVICE_NAME}.service"

# Set Permissions
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"
chmod -R 750 "${INSTALL_DIR}"

# 7. Reload and Enable Service
echo "[+] Reloading systemd daemon..."
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"

echo "=============================================================================="
echo " [SUCCESS] Headless VPS deployment configured successfully!"
echo "=============================================================================="
echo "To start the 30-day soak testing service, run:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo ""
echo "To check service status and logs:"
echo "  sudo systemctl status ${SERVICE_NAME}"
echo "  journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "To monitor real-time daily telemetry:"
echo "  tail -f ${DATA_DIR}/ban_risk_telemetry.csv"
echo "=============================================================================="
