#!/bin/bash

set -e  # Exit immediately if a command exits with non-zero status

# Define colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Configuration
INSTALL_DIR="./"
SERVICE_NAME="tec-data-collector"
ENDPOINT="capacity_operationally-available"
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

# Parse database configuration from config.yaml
log "Reading database configuration from $CONFIG_FILE"
if command -v python3 &> /dev/null; then
	# Extract database configuration using Python
	DB_CONFIG=$(python3 -c "
import yaml
with open('$CONFIG_FILE', 'r') as file:
	config = yaml.safe_load(file)
db = config.get('Database', {})
print(f\"DB_TYPE={db.get('db_type', 'postgresql')}\")
print(f\"DB_HOST={db.get('db_host', 'localhost')}\")
print(f\"DB_PORT={db.get('db_port', 5432)}\")
print(f\"DB_NAME={db.get('db_name', 'tec_data')}\")
print(f\"DB_USER={db.get('db_user', 'postgres')}\")
print(f\"DB_PASSWORD={db.get('db_password', 'password')}\")
endpoints = config.get('endpoint_table_mappings', {})
print(f\"ENDPOINTS={endpoints.get('capacity_operationally-available', 'capacity_operationally-available')}\")
")
	
	# Source the database configuration
	eval "$DB_CONFIG"
	log "Successfully read database configuration"
else
	error "Python3 is required to parse YAML configuration file"
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
log "Setting up database user..."
sudo -u postgres psql -c "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1 || \
	sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD' SUPERUSER;"

# Create database if it doesn't exist
log "Creating database if it doesn't exist..."
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 || \
	sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"

# Initialize database with schema
log "Initializing database schema..."
SCHEMA_FILE="$INSTALL_DIR/src/staging_schema.sql"
if [ -f "$SCHEMA_FILE" ]; then
	PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$SCHEMA_FILE"
	log "Database schema initialized successfully"
else
	error "Schema file not found at $SCHEMA_FILE"
fi

# Create service user if it doesn't exist
log "Creating service user..."
id -u datauser &>/dev/null || useradd -m -s /bin/bash datauser

# Ensure correct permissions
log "Setting permissions..."
mkdir -p $INSTALL_DIR/data $INSTALL_DIR/logs
chown -R datauser:datauser $INSTALL_DIR
chmod -R 755 $INSTALL_DIR

# Create a correct systemd service file
log "Creating systemd service file with correct paths..."
CURRENT_DIR=$(readlink -f "$INSTALL_DIR")
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME.service"

# Create new service file with absolute paths and correct command format
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=TEC Data Collection Service
After=network.target postgresql.service

[Service]
User=datauser
WorkingDirectory=$CURRENT_DIR
ExecStart=/usr/bin/python3 $CURRENT_DIR/src/main.py --endpoint $ENDPOINTS
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

log "Service file created with correct paths"

# Reload systemd, enable and start the service
log "Enabling and starting service..."
systemctl daemon-reload
systemctl enable $SERVICE_NAME
systemctl start $SERVICE_NAME

# Check if service started successfully
if systemctl is-active --quiet $SERVICE_NAME; then
	log "Service $SERVICE_NAME started successfully"
else
	warn "Service $SERVICE_NAME failed to start. Check logs with: journalctl -u $SERVICE_NAME"
fi

# Show service status
log "Service status:"
systemctl status $SERVICE_NAME --no-pager

log "Installation completed successfully!"
log "To check logs: journalctl -u $SERVICE_NAME -f"
log "To manage service: systemctl [start|stop|restart|status] $SERVICE_NAME"