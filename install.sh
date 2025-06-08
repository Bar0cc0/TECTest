#!/bin/bash

set -e

# Define colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
INSTALL_DIR="./" # Assuming the script is run from the TecTest directory
SERVICE_NAME="tec-data-collector"
# ENDPOINT is now read from config.yaml
CONFIG_FILE="$(dirname "$(readlink -f "$0")")/config.yaml"

# Function to display status messages
log() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then
    error "This script must be run as root or with sudo privileges"
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    error "Config file not found at $CONFIG_FILE"
fi

# Parse database configuration and endpoints from config.yaml
log "Reading configuration from $CONFIG_FILE"
if command -v python3 &> /dev/null && command -v pip3 &> /dev/null && python3 -c "import yaml" &> /dev/null; then
    # Extract database configuration using Python
    DB_CONFIG_AND_ENDPOINTS=$(python3 -c "
import yaml
import sys
try:
    with open('$CONFIG_FILE', 'r') as file:
        config = yaml.safe_load(file)
    db = config.get('Database', {})
    print(f\"DB_TYPE={db.get('db_type', 'postgresql')}\")
    print(f\"DB_HOST={db.get('db_host', 'localhost')}\")
    print(f\"DB_PORT={db.get('db_port', 5432)}\")
    print(f\"DB_NAME={db.get('db_name', 'tec_data')}\")
    print(f\"DB_USER={db.get('db_user', 'postgres')}\")
    print(f\"DB_PASSWORD={db.get('db_password', 'password')}\")
    
    # Get the first endpoint from mappings or a default
    endpoint_mappings = config.get('endpoint_table_mappings', {})
    if endpoint_mappings and isinstance(endpoint_mappings, dict) and len(endpoint_mappings) > 0:
        first_endpoint = list(endpoint_mappings.keys())[0]
    else:
        first_endpoint = 'capacity_operationally-available' # Default if no endpoints found
    print(f\"ENDPOINTS={first_endpoint}\")

except FileNotFoundError:
    sys.stderr.write('Error: Config file not found by Python script.\\n')
    sys.exit(1)
except Exception as e:
    sys.stderr.write(f'Error parsing config file with Python: {e}\\n')
    sys.exit(1)
")
    
    # Source the database configuration and endpoints
    eval "$DB_CONFIG_AND_ENDPOINTS"
    log "Successfully read configuration"
else
    error "Python3, pip3, and PyYAML are required to parse YAML configuration file. Please install them (e.g., sudo apt install python3 python3-pip && pip3 install PyYAML)."
fi

# Install PostgreSQL if not already installed
log "Checking PostgreSQL installation..."
if ! command -v psql &> /dev/null; then
    log "PostgreSQL not found. Installing..."
    apt-get update
    apt-get install -y postgresql postgresql-contrib
    systemctl enable postgresql
    systemctl start postgresql
    log "PostgreSQL installed successfully"
else
    log "PostgreSQL is already installed"
fi

# Check if PostgreSQL service is running
log "Checking PostgreSQL service status..."
if ! systemctl is-active --quiet postgresql; then
    log "Starting PostgreSQL service..."
    systemctl start postgresql
fi

# Set PostgreSQL password to match config.yaml
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '$DB_PASSWORD';"

# Create database user if it doesn't exist
log "Setting up database user '$DB_USER'..."
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1; then
    log "User '$DB_USER' does not exist. Creating user..."
    sudo -u postgres psql -c "CREATE USER \"$DB_USER\" WITH PASSWORD '$DB_PASSWORD' SUPERUSER;"
    log "User '$DB_USER' created."
else
    log "User '$DB_USER' already exists."
    # Optionally, update password if needed, or ensure privileges
    sudo -u postgres psql -c "ALTER USER \"$DB_USER\" WITH PASSWORD '$DB_PASSWORD' SUPERUSER;"
fi

# Create database if it doesn't exist
log "Creating database '$DB_NAME' if it doesn't exist..."
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    sudo -u postgres psql -c "CREATE DATABASE \"$DB_NAME\" OWNER \"$DB_USER\";"
    log "Database '$DB_NAME' created."
else
    log "Database '$DB_NAME' already exists."
fi

# Initialize database with schema
log "Initializing database schema..."
# Resolve INSTALL_DIR to an absolute path for reliability
ABS_INSTALL_DIR=$(readlink -f "$INSTALL_DIR")
SCHEMA_FILE="$ABS_INSTALL_DIR/src/staging_schema.sql"

if [ -f "$SCHEMA_FILE" ]; then
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U "$DB_USER" -d "$DB_NAME" -f "$SCHEMA_FILE"
    log "Database schema initialized successfully"
else
    error "Schema file not found at $SCHEMA_FILE"
fi

# Create service user if it doesn't exist
id -u datauser &>/dev/null || useradd -m -s /bin/bash datauser

# Create a systemd service file
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME.service"

# Create new service file with absolute paths and correct command format
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=TEC Data Collection Service
After=network.target postgresql.service

[Service]
User=datauser
WorkingDirectory=$CURRENT_DIR
# LOGS_DIR environment variable removed
ExecStart=/usr/bin/python3 $CURRENT_DIR/src/main.py $ENDPOINTS
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

log "Service file created with correct paths"
