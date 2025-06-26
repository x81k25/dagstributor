# Dagstributor

A Dagster-based data processing and orchestration infrastructure for automated media distribution workflows.

## Overview

Dagstributor is a comprehensive data pipeline system that automates the ingestion, processing, and distribution of media content. It leverages Dagster for workflow orchestration and runs on Kubernetes with environment-specific configurations.

## Architecture

The system consists of 10 sequential jobs that form a complete data processing pipeline:

1. **RSS Ingest** - Fetches content from RSS feeds
2. **Collect** - Gathers data from ingested feeds
3. **Parse** - Processes and extracts structured data
4. **File Filtration** - Filters files based on criteria
5. **Metadata Collection** - Extracts and enriches metadata
6. **Media Filtration** - Filters media content
7. **Initiation** - Initiates download processes
8. **Download Check** - Monitors download progress
9. **Transfer** - Transfers completed downloads
10. **Cleanup** - Removes temporary files and data

### Wiring Schema Tics Module

The `wiring_schema_tics` module provides database schema management capabilities for the system:

#### Available Jobs

1. **test_db_connection_job** - Tests database connectivity with a simple query
2. **wst_bak_atp_job** - Executes all backup ATP scripts for schema backup and restoration

#### SQL Scripts Organization

- **ddl/** - Data Definition Language scripts for schema creation
  - `00_drop_schema.sql` - Drops existing schemas
  - `01_instantiate_media.sql` - Creates media schema and tables
  - `02_instantiate_training.sql` - Creates training schema and tables
  - `03_instantiate_prediction.sql` - Creates prediction schema and tables

- **bak/** - Backup and restore scripts
  - `bak_media.sql` - Backs up media schema
  - `bak_training.sql` - Backs up training schema
  - `bak_prediction.sql` - Backs up prediction schema
  - `reload_media.sql` - Restores media schema from backup
  - `reload_training.sql` - Restores training schema from backup
  - `reload_prediction.sql` - Restores prediction schema from backup

- **test.sql** - Simple database connection test script

#### SQL Execution Pattern

All ops in the wiring_schema_tics module follow a consistent pattern for executing SQL scripts:

1. **Script Loading**: SQL files are loaded from the `sql/` directory relative to the ops module
2. **Database Connection**: Uses psycopg2 with environment variables (WST_PGSQL_*)
3. **Execution Strategy**:
   - Scripts with dollar-quoted strings (`$$`) are executed as a single block
   - Other scripts are split on semicolons and executed statement by statement
   - Autocommit is enabled for DDL operations
4. **Error Handling**: Detailed logging of each statement execution with proper error reporting
5. **Result Collection**: 
   - SELECT statements return rows as RealDictCursor results
   - Non-SELECT statements report affected row counts
   - All ops return structured output with execution metadata

Example pattern used by all ops:
```python
def execute_sql_file(context, sql_filename):
    # 1. Load SQL file from sql/ directory
    # 2. Connect to database using environment variables
    # 3. Execute statements (single block for $$ or split on ;)
    # 4. Return results with metadata
```

This standardized approach ensures consistent behavior across all database operations.

## Project Structure

```
dagstributor/
├── config/
│   ├── dagster.yaml             # Dagster infrastructure configuration
│   └── schedules/               # Environment-specific schedule configs
│       ├── base.yaml            # Base schedule configuration
│       ├── dev.yaml             # Development overrides
│       ├── stg.yaml             # Staging overrides
│       └── prod.yaml            # Production overrides
├── dagstributor/
│   ├── automatic_transmission/
│   │   ├── __init__.py
│   │   ├── assets.py            # Asset definitions
│   │   ├── ops.py               # Operation implementations
│   │   ├── jobs.py              # Job definitions
│   │   ├── schedules.py         # Schedule configurations
│   │   └── config_loader.py     # YAML config loader
│   └── wiring_schema_tics/
│       ├── __init__.py
│       ├── ops.py               # Database operation implementations
│       ├── jobs.py              # Database management jobs
│       └── sql/                 # SQL scripts
│           ├── bak/             # Backup and restore scripts
│           ├── ddl/             # Schema definition scripts
│           └── test.sql         # Database connection test
├── repositories/
│   └── main.py                  # Main Dagster repository
├── tests/                       # Unit and integration tests
├── requirements.txt             # Python dependencies
└── setup.py                     # Package setup configuration
```

## Environment Configuration

The system supports three environments with GitOps-based configuration:
- **dev** - Development environment
- **stg** - Staging environment  
- **prod** - Production environment

Schedule configurations are loaded based on the `ENVIRONMENT` variable and can be customized per environment through YAML files.

### Database Configuration

The wiring_schema_tics module requires the following PostgreSQL environment variables:
- `WST_PGSQL_HOST` - Database host
- `WST_PGSQL_PORT` - Database port (default: 5432)
- `WST_PGSQL_DATABASE` - Database name
- `WST_PGSQL_USERNAME` - Database username
- `WST_PGSQL_PASSWORD` - Database password

## Local Development

### Prerequisites

- Python 3.8+
- pip
- Access to Kubernetes cluster (for deployment testing)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/x81k25/dagstributor.git
   cd dagstributor
   ```

2. Switch to dev branch:
   ```bash
   git checkout dev
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -e .
   pip install -e .[dev]  # For development dependencies
   ```

4. Start Dagster web server:
   ```bash
   dagster-webserver -f repositories/main.py
   ```

5. Access Dagster UI at http://localhost:3000

### Running Jobs Locally

```bash
# List all available jobs
dagster job list -f repositories/main.py

# Execute a specific job
dagster job execute -f repositories/main.py -j at_01_rss_ingest_job

# Execute database management jobs
dagster job execute -f repositories/main.py -j test_db_connection_job
dagster job execute -f repositories/main.py -j wst_bak_atp_job

# Run with custom config
dagster job execute -f repositories/main.py -j at_01_rss_ingest_job -c config.yaml
```

## Deployment

### Kubernetes Deployment

The application runs on Kubernetes with different namespaces per environment:
- `media-dev` - Development namespace
- `media-stg` - Staging namespace
- `media-prod` - Production namespace

### Accessing Deployed Instances

#### Development Environment
```bash
# Access Dagster UI (NodePort service)
http://192.168.50.2:30302

# Get current dagster pod
kubectl get pods -n media-dev | grep dagster

# Execute job via kubectl
kubectl exec -n media-dev <dagster-pod> -c dagster-dagit -- sh -c \
  "cd /opt/dagster/app && PYTHONPATH=/opt/dagster/app dagster job execute -m repositories.main -j at_01_rss_ingest_job"
```

### Restarting Pods

```bash
# Restart development pod
kubectl delete pod -n media-dev $(kubectl get pods -n media-dev | grep dagster | awk '{print $1}')

# Restart staging pod  
kubectl delete pod -n media-stg $(kubectl get pods -n media-stg | grep dagster | awk '{print $1}')

# Restart production pod
kubectl delete pod -n media-prod $(kubectl get pods -n media-prod | grep dagster | awk '{print $1}')
```

## Schedule Configuration

Schedules are configured via YAML files in `config/schedules/`. Each environment can have custom:
- **cron_schedule** - Cron expression for timing
- **default_status** - RUNNING or STOPPED
- **enabled** - Enable/disable schedule

Example configuration:
```yaml
schedules:
  at_01_rss_ingest:
    enabled: true
    cron_schedule: "0 * * * *"  # Every hour
    default_status: "RUNNING"
```

## Testing

Run the test suite:
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=dagstributor

# Run specific test file
pytest tests/test_ops.py
```

## Development Guidelines

1. **Branch Strategy**:
   - `dev` - Active development
   - `stg` - Staging releases
   - `main` - Production releases

2. **Code Style**:
   - Follow PEP 8
   - Use type hints
   - Run formatters: `black .` and `isort .`
   - Lint code: `flake8 .` and `mypy .`

3. **Adding New Jobs**:
   - Define ops in `ops.py`
   - Create job in `jobs.py`
   - Add schedule in `schedules.py`
   - Update schedule configs in YAML files

## CI/CD

The repository uses GitHub Actions for CI/CD with automatic deployments:
- Push to `dev` → Deploy to development
- Push to `stg` → Deploy to staging
- Push to `main` → Deploy to production

## Monitoring

- Dagster UI provides real-time job monitoring
- Job logs are available in Kubernetes pod logs
- Metrics and alerts can be configured via Dagster sensors

## Troubleshooting

### Common Issues

1. **Pod CrashLoopBackOff**:
   ```bash
   kubectl logs -n media-dev <pod-name> -c dagster-dagit
   ```

2. **Schedule Not Running**:
   - Check schedule configuration in YAML
   - Verify ENVIRONMENT variable is set correctly
   - Check Dagster UI for schedule status

3. **Import Errors**:
   - Ensure all dependencies are installed
   - Check PYTHONPATH includes `/opt/dagster/app`

## Contributing

1. Fork the repository
2. Create a feature branch from `dev`
3. Make changes and add tests
4. Submit a pull request to `dev` branch

## License

[Specify your license here]

## Support

For issues and feature requests, please use the [GitHub Issues](https://github.com/x81k25/dagstributor/issues) page.