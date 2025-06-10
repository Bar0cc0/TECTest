# TEC_TEST Data Pipeline

[1. Information](#information)  
[2. Overview](#overview)  
[3. Installation](#installation)  
[4. Running the Application](#running-the-application)  
[5. Configuration](#configuration)  
[6. Test Suite](#test-suite)  
[7. Database Tables](#database-tables)  
[8. Observability and Monitoring](#observability-and-monitoring)

## Information
**Energy Transfer Capacity Data Pipeline**
- Application Name: TEC Data Pipeline v1.0.0
- Configuration: `config.yaml`, `.env` (for Docker)
- Entry Point: `src/main.py`

## Overview

- This app is designed to download and process CSV files from Energy Transfer, and store the data into a PostgreSQL database. 
- Once started, the system continuously checks for new data cycles and processes them as they become available. 
- Target is TW's operational capacity but the system can easily support additional data sources (via `config.yaml`).
- It supports containerized deployment using Docker and systemd for process management.


## Installation
### Prerequisites
- Python 3.10+ 
- PostgreSQL, PGAdmin (optional)
- Docker, Docker Desktop (optional)
- Anaconda (optional)

### Clone the Repository
```bash
git clone https://github.com/Bar0cc0/TECTest.git
cd TecTest
```
### Create a Virtual Environment (recommended)
```bash
python3 -m venv .tecEnv       # Or use Anaconda: conda create -n tecEnv python=3.10
source .tecEnv/bin/activate   #                  conda activate tecEnv
```
### Install Dependencies
```bash
pip install -r requirements.txt
```

## Running the Application

### Method 1: Docker Deployment (recommended)

#### Building and Starting Services
```bash
# Build all services
docker-compose build

# Start services
docker-compose up

# Stop services (and delete database data by removing volumes)
docker-compose down -v

```


### Method 2: Direct CLI Usage (local, for development or testing)
### Syntax

```bash
python src/main.py [--params key=value [key=value ...]] [--loglevel LEVEL] [--test]
```

### Examples

```bash
# Fetch data for default asset/endpoint (defined in config.yaml)
python src/main.py

# Fetch specific cycle for default asset/endpoint
python src/main.py --params cycle=1

# Enable Debug Logging
python src/main.py --loglevel DEBUG

# Run the Test Suite
python src/main.py --test

# NOTE: On WSL, you might encounter permission issues
```


### Method 3: Interactive mode (local, for development or testing)
```bash
bash install.sh
```
<img src="./static/CLIMenu.png" alt="interactive" width="500" />

## Configuration

The system is configurable via a YAML file (`config.yaml`).  

However, in production (e.g., Docker), environment variables (`.env`) will have precedence over `config.yaml` settings to ensure sensitive information is not exposed.

## Test Suite

1. **Test Coverage**
   - Unit tests cover key components:
	 - Connectors
	 - Scheduler

2. **Reports**
   - Pytest logs are displayed in the terminal.
   - HTML coverarge report available at `tests/coverage_html/index.html` (running a local server is required to view in browser)

![Test Coverage](./static/TestSuite.png)


## Database Tables
Schema structure is defined in `src/staging_schema.sql`.  
Data loading is handled `DatabaseConnector.postdata()` through a merge operation, checking a unique key constraint to avoid duplicates.:
```sql
-- Parametrized SQL query for merging data into a PostgreSQL table
INSERT INTO {schema}.{table_name} (column1, column2, ...)
VALUES (%s, %s, ...)
ON CONFLICT (key_columns)
DO UPDATE SET 
    non_key_column1 = EXCLUDED.non_key_column1,
    non_key_column2 = EXCLUDED.non_key_column2,
    ...
```
**Limitation** Current staging environment does not support SCD; however, for production, enhancements would include adding columns:
```sql
-- SCD Type 2 version tracking
version_id SERIAL PRIMARY KEY,
is_current BOOLEAN NOT NULL DEFAULT TRUE,
valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
source_system VARCHAR(50),
```


## Observability and Monitoring
### Docker Desktop
Containers can be easily managed using Docker Desktop: 
![Docker Desktop](./static/DockerDesktop.png)

### PGAdmin
The database table can be managed using PGAdmin or any PostgreSQL client:  
![Database Tables](./static/PGAdmin.png)

### Project Assets
![Assets](./static/Assets.png)