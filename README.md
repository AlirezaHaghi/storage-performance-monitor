# Storage Performance Monitor

A Python-based storage performance monitoring application that collects and analyzes performance metrics for storage pools, LUNs, and network interfaces.

## System Requirements

This application requires the following system tools to be installed:

### Required Packages

- **sysstat**: Provides the `iostat` command for disk I/O statistics

  - Install on Ubuntu/Debian: `sudo apt install sysstat`
  - Install on CentOS/RHEL: `sudo yum install sysstat` or `sudo dnf install sysstat`
  - Install on Arch: `sudo pacman -S sysstat`
- **lvm2**: Provides the `lvs` command for logical volume management

  - Install on Ubuntu/Debian: `sudo apt install lvm2`
  - Install on CentOS/RHEL: `sudo yum install lvm2` or `sudo dnf install lvm2`
  - Install on Arch: `sudo pacman -S lvm2`

### Python Dependencies

The application uses Python 3.11+ and the following packages (managed by uv):

- FastAPI
- SQLModel/SQLAlchemy
- asyncio

## Installation

1. Clone the repository
2. Install Python dependencies:
   ```bash
   uv sync
   ```
3. Ensure required system packages are installed:
   ```bash
   sudo apt install sysstat lvm2  # Ubuntu/Debian
   ```
4. Verify required commands are available:
   ```bash
   which iostat
   which lvs
   ```

## Usage

### Starting the API Server

```bash
uv run uvicorn performance_monitor.monitor:app
```

### Starting the Data Collector

```bash
uv run python -m performance_monitor.collector
```

## Features

- Real-time storage performance monitoring
- Pool and LUN performance metrics collection
- Network interface monitoring (Ethernet and Fiber Channel)
- RESTful API for data retrieval
- Automatic data cleanup and retention management

## Configuration

Performance monitoring intervals and data retention policies can be configured in `performance_monitor/config.py`.
