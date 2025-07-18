# Dagstributor

A Dagster-based data processing and orchestration infrastructure for automated media distribution workflows. Deployed via Docker images with GitOps automation.

## Overview

Dagstributor is a comprehensive data pipeline system that automates the ingestion, processing, and distribution of media content. It leverages Dagster for workflow orchestration and runs on Kubernetes with environment-specific configurations.

## Architecture

The system consists of two main components:

### Automatic Transmission Pipeline (11 jobs)

Sequential data processing pipeline:
1. **at_01_rss_ingest_job** - Fetches content from RSS feeds
2. **at_02_collect_job** - Gathers data from ingested feeds
3. **at_03_parse_job** - Processes and extracts structured data
4. **at_04_file_filtration_job** - Filters files based on criteria
5. **at_05_metadata_collection_job** - Extracts and enriches metadata
6. **at_06_media_filtration_job** - Filters media content
7. **at_07_initiation_job** - Initiates download processes
8. **at_08_download_check_job** - Monitors download progress
9. **at_09_transfer_job** - Transfers completed downloads
10. **at_10_cleanup_job** - Removes temporary files and data
11. **at_full_pipeline_job** - Executes the complete pipeline sequence

### Wiring Schema Tics (6 jobs)

Database schema management system for PostgreSQL ATP (Automatic Transmission Pipeline) database operations.

### Wiring Schema Tics Module

The `wiring_schema_tics` module provides database schema management capabilities for the system:

#### Available Jobs

1. **test_db_connection_job** - Tests database connectivity with a simple query
2. **wst_atp_bak_job** - Executes all backup ATP scripts for schema backup
3. **wst_atp_drop_job** - Drops the atp schema (WARNING: deletes all data!)
4. **wst_atp_instantiate_job** - Creates the atp schema with all tables and permissions
5. **wst_atp_reload_job** - Restores data from backup tables into atp schema
6. **wst_atp_bak_drop_reload_job** - Complete backup, drop, instantiate, and reload sequence

#### SQL Scripts Organization

- **ddl/** - Data Definition Language scripts for schema creation
  - `00_drop_schema.sql` - Drops existing atp schema
  - `01_instantiate_media.sql` - Creates media tables within atp schema
  - `02_instantiate_training.sql` - Creates training tables within atp schema
  - `03_instantiate_prediction.sql` - Creates prediction tables within atp schema
  - `10_set_perms.sql` - Sets permissions on atp schema objects

- **bak/** - Backup and restore scripts
  - `bak_media.sql` - Backs up atp.media table
  - `bak_training.sql` - Backs up atp.training table
  - `bak_prediction.sql` - Backs up atp.prediction table
  - `reload_media.sql` - Restores atp.media table from backup
  - `reload_training.sql` - Restores atp.training table from backup
  - `reload_prediction.sql` - Restores atp.prediction table from backup

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
│   └── schedules/               # Environment-specific schedule configs
│       ├── dev.yaml             # Development schedule settings
│       ├── stg.yaml             # Staging schedule settings
│       └── prod.yaml            # Production schedule settings
├── dagstributor/
│   ├── automatic_transmission/  # Automatic Transmission pipeline (10 jobs)
│   │   ├── __init__.py
│   │   ├── config_loader.py     # YAML schedule config loader
│   │   ├── jobs.py              # Job definitions (at_01 through at_10 + full pipeline)
│   │   ├── ops.py               # K8s operation implementations
│   │   └── schedules.py         # Schedule configurations
│   ├── wiring_schema_tics/      # Database schema management (6 jobs)
│   │   ├── __init__.py
│   │   ├── jobs.py              # Database management jobs
│   │   ├── ops.py               # Database operation implementations
│   │   ├── schedules.py         # WST schedule configurations
│   │   └── sql/                 # SQL scripts directory
│   │       ├── bak/             # Backup and restore scripts
│   │       └── ddl/             # Schema definition scripts
│   ├── definitions.py           # Main Dagster definitions
│   └── resources/               # Resource configurations
├── repositories/
│   └── main.py                  # Main Dagster repository
├── pyproject.toml               # Python project configuration
├── requirements.txt             # Python dependencies
└── workspace.yaml               # Dagster workspace configuration
```

## Configuration Management

**⚠️ Important: Dagster configuration is managed entirely by Kubernetes manifests and environment variables.**

- **No local `dagster.yaml` file is used** - Dagster auto-generates configuration from K8s environment
- All Dagster settings (run launcher, storage, monitoring) are controlled via K8s ConfigMaps and Secrets
- Environment variables like `DAGSTER_PG_*` and `DAGSTER_K8S_*` drive the configuration
- Any infrastructure changes (timeouts, launchers, storage) must be made in K8s manifests, not application code

This ensures:
- Consistent configuration across environments
- GitOps-managed infrastructure settings
- No configuration drift between deployments
- Simplified container builds

## Deployment & Environment Configuration

### GitOps Docker Deployment

The system is deployed via Docker images with automatic GitOps workflows:

#### Branch-Environment Mapping
- **dev** branch → Development environment 
- **stg** branch → Staging environment
- **main** branch → Production environment

#### Automated Pipeline
1. **Code Push** → Triggers GitHub Actions workflow
2. **Docker Build** → Creates image from `dagster/dagster-k8s:1.10.20` base
3. **Validation Tests** → Runs comprehensive Dagster component tests
4. **Image Push** → Only if tests pass, pushes to `ghcr.io/x81k25/dagstributor`
5. **ArgoCD Deployment** → Automatically updates Kubernetes manifests and deploys

#### Image Tags
- **Branch tag**: `ghcr.io/x81k25/dagstributor:dev|stg|main`
- **SHA tag**: `ghcr.io/x81k25/dagstributor:sha-<7-char-commit>`

#### Validation Tests
The build pipeline validates:
- All Python imports load correctly
- Repository definition loads (17 jobs + 10 schedules)
- All ops have valid configurations
- workspace.yaml structure is correct

### Environment Configuration

Schedule configurations are loaded based on the `ENVIRONMENT` variable and can be customized per environment through YAML files in `config/schedules/`.

### Database Configuration

The wiring_schema_tics module requires the following PostgreSQL environment variables:
- `WST_PGSQL_HOST` - Database host
- `WST_PGSQL_PORT` - Database port (default: 5432)
- `WST_PGSQL_DATABASE` - Database name
- `WST_PGSQL_USERNAME` - Database username
- `WST_PGSQL_PASSWORD` - Database password

### Kubernetes Configuration

The Automatic Transmission jobs use the following Kubernetes resources:
 
#### ConfigMaps
- `at-config` - AT-specific configuration settings
- `environment` - Environment-specific settings
- `transmission-config` - Transmission service configuration
- `rear-diff-config` - Rear diff service configuration
- `reel-driver-config` - Reel driver service configuration
- `wst-config` - Wiring schema tics configuration

#### Secrets
- `at-secrets` - Sensitive AT configuration (renamed from `at-sensitive`)
- `transmission-secrets` - Transmission service secrets
- `rear-diff-secrets` - Rear diff service secrets
- `wst-secrets` - Wiring schema tics secrets

#### Environment Variables
AT jobs use the following prefixed environment variables:
- `AT_DOWNLOAD_DIR` - Download directory path
- `AT_MOVIE_DIR` - Movie storage directory path
- `AT_TV_SHOW_DIR` - TV show storage directory path

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

# Execute wiring schema tics jobs
dagster job execute -f repositories/main.py -j test_db_connection_job
dagster job execute -f repositories/main.py -j wst_atp_bak_job
dagster job execute -f repositories/main.py -j wst_atp_drop_job
dagster job execute -f repositories/main.py -j wst_atp_instantiate_job
dagster job execute -f repositories/main.py -j wst_atp_reload_job
dagster job execute -f repositories/main.py -j wst_atp_bak_drop_reload_job

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
    
  wst_atp_bak:
    enabled: true
    cron_schedule: "57 4 * * 0"  # Weekly on Sunday at 4:57 AM (prod)
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