# TEC_TEST Data Pipeline

## Information
**Energy Transfer Capacity Data Pipeline**
- Initial URL: https://twtransfer.energytransfer.com
- Configuration: config.yaml
- Client Application Name: TEC Data Pipeline v1.0.0


## Overview

- This app is a Python-based system designed to connect to the Energy Transfer, download and process the csv files, and store data into a PostgreSQL database. 
- Once started, the system continuously checks for new data cycles and processes them as they become available. 
- Targeted endpoint is **Operational Capacity** but the system can be easily extended to support additional data sources.
- It is built with a focus on modularity, configurability, and performance, and it supports containerized deployment using Docker.



## Command Line Usage

The application provides a flexible command-line interface for data retrieval and processing.

### Syntax

```bash
python src/main.py [--params key=value [key=value ...]] [--loglevel LEVEL] [--test]
```

### Examples

```bash
# Fetch Operational Capacity data (default endpoint)
python src/main.py

# Fetch specific cycle for Operational Capacity
python src/main.py --params cycle=1

# Enable Debug Logging
python src/main.py --loglevel DEBUG

# Run the Test Suite
python src/main.py --test
```

## Installation

### Prerequisites

- Python 3.10+
- pip
- PostgreSQL for database integration
- Docker and Docker Compose (for containerized deployment)

### Method 1: Local Installation

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd TecTest
   ```

2. **Install dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Run the install script**
   ```bash
   chmod +x install.sh
   ./install.sh
   ```

### Method 2: Docker Deployment

1. **Build the Docker image**
   ```bash
   docker-compose build
   ```

2. **Run the application in containers**
   ```bash
   docker-compose up
   ```

## Configuration

The system is configurable via a YAML file (`config.yaml`). 

## Test Suite

1. **Test Coverage**
   - Unit tests cover key components:
     - Connectors
     - Scheduler

2. **Test Organization**
   - Tests separated by component
   - Shared fixtures for consistent testing environments
   - Independent test execution with automatic resource cleanup

3. **Coverage Reports**
   - Generated using pytest-cov
   - HTML reports available in tests/coverage_html/

## Docker Deployment

The application is containerized for easy deployment and includes:

1. **Multi-Container Setup**
   - Application container with the Python pipeline
   - PostgreSQL container for data storage
   - Volumes for persistent data

2. **Environment Configuration**
   - Environment variables for sensitive information
   - Docker-specific overrides for networking (e.g., database host)

3. **Deployment Commands**
   - Build: `docker-compose build`
   - Run: `docker-compose up`
   - View logs: `docker-compose logs -f`
   - Stop: `docker-compose down`

4. **Resource Management**
   - Container health checks
   - Automatic restarts on failure
   - Named volumes for data persistence