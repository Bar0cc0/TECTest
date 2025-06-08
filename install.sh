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
log "Creating service user 'datauser'..."
id -u datauser &>/dev/null || useradd -m -s /bin/bash datauser

# Ensure correct permissions on the application directory
log "Setting permissions for application directory..."
# CURRENT_DIR should be the absolute path to the application's root
CURRENT_DIR=$(readlink -f "$INSTALL_DIR") 
mkdir -p $CURRENT_DIR/data $CURRENT_DIR/logs
chown -R datauser:datauser $CURRENT_DIR
chmod -R 755 $CURRENT_DIR # datauser needs execute to cd, read for files

# Create a systemd service file
log "Creating systemd service file with correct paths..."
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

# Create the systemd log directory (for systemd's own journal logging, not for LOGS_DIR env var)
# This directory is still useful if your Python app logs to stdout/stderr, which systemd captures.
log "Creating system log directory for service (if needed by systemd)..."
mkdir -p /var/log/tec-data 
chown -R datauser:datauser /var/log/tec-data
chmod -R 755 /var/log/tec-data

# Reload systemd and enable service (but don't start it yet)
log "Enabling service..."
systemctl daemon-reload
systemctl enable $SERVICE_NAME

# Function to display the menu and get user choice
show_menu() {
    clear
    echo -e "${BLUE}=======================================${NC}"
    echo -e "${BLUE}       TEC Data Pipeline Menu         ${NC}"
    echo -e "${BLUE}=======================================${NC}"
    echo ""
    echo -e "  ${GREEN}1)${NC} Run as a systemd service (background)"
    echo -e "  ${GREEN}2)${NC} Run directly in this terminal (foreground)"
    echo -e "  ${GREEN}3)${NC} Launch test mode"
    echo -e "  ${GREEN}4)${NC} Exit"
    echo ""
    echo -e "${BLUE}=======================================${NC}"
    echo -n "Please enter your choice [1-4]: "
    read -r choice

    case $choice in
        1)
            # Run the application as a systemd service
            run_service
            ;;
        2)
            # Run the application directly in the terminal
            run_directly
            ;;   
        3)
            # Launch test mode
            launch_test_mode
            ;;
        4)
            echo -e "${GREEN}Exiting. Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${YELLOW}Invalid option. Please try again.${NC}"
            sleep 2
            show_menu
            ;;
    esac
}

# Function to run systemd service in the background
run_service() {
    # Start the systemd service
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
    
    log "To check logs: journalctl -u $SERVICE_NAME -f"
    log "To manage service: systemctl [start|stop|restart|status] $SERVICE_NAME"

    # Show data paths
    echo ""
    log "Data files location: $CURRENT_DIR/data"
    log "Application log files location (default): $CURRENT_DIR/logs (or as configured in Python app)" 

    # Ask if user wants to see the logs (systemd journal logs)
    echo ""
    echo -n "Would you like to see the live systemd journal logs? (y/n): "
    read -r view_logs
    if [[ "$view_logs" =~ ^[Yy] ]]; then
        log "Showing live systemd journal logs (press Ctrl+C to exit)..."
        journalctl -u $SERVICE_NAME -f
    fi
    
    echo ""
    read -p "Press Enter to return to the menu..."
    show_menu
}


# Function to run the application directly in the terminal
run_directly() {
    log "Running TEC data pipeline directly (press Ctrl+C to stop)..."
    echo -e "${YELLOW}--------------------------------------------${NC}"
    sudo -u datauser python3 $CURRENT_DIR/src/main.py $ENDPOINTS
    echo -e "${YELLOW}--------------------------------------------${NC}"
    
    echo ""
    read -p "Press Enter to return to the menu..."
    show_menu
}

# Function to launch test mode
launch_test_mode() {
    log "Launching TEC data pipeline in test mode..."
    local func_current_dir
    func_current_dir=$(readlink -f "$INSTALL_DIR")
    
    # Run the main script in test mode
    echo -e "${YELLOW}Running in test mode:${NC}"
    echo -e "${YELLOW}--------------------------------------------${NC}"
    cd "$func_current_dir"
    sudo -u datauser python3 "$func_current_dir/src/main.py" --test $ENDPOINTS
    echo -e "${YELLOW}--------------------------------------------${NC}"
    
    echo ""
    read -p "Press Enter to return to the menu..."
    show_menu
}

# Display the interactive menu
show_menu