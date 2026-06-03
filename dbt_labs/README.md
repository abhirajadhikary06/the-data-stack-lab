# What is dbt (data build tool)?

**dbt (Data Build Tool)** is an open-source command-line tool and cloud-hosted compilation engine that acts as the orchestration layer for executing modular, version-controlled SQL statements directly inside an ELT (Extract, Load, Transform) cloud data warehouse.

### 1. Install dbt

Installed via `pip install dbt-core` or a warehouse-specific adapter:

- `dbt-snowflake`
- `dbt-bigquery`
- `dbt-duckdb`

### 2. Initialize a dbt Project

Run `dbt init` to scaffold the project structure (`models/`, `macros/`, `tests/`, etc.).

### 3. Configure Warehouse Connections (Dev & Prod)

`profiles.yml` - connection and environment settings.
Edit `profiles.yml` to define multiple targets (`dev`, `prod`) with credentials, schema, and database.

`dbt_project.yml` - project behaviour and model configuration
Edit `dbt_project.yml` to define behaviour of your model like views, tables, packages path, schemas, tags.

### 4. Common commands in dbt
- `dbt debug` - to verify that environment for dbt is setup correctly
- `dbt compile` - generates the executable SQL from dbt models. These compilations are not run against the warehouse, they are compiled and kept in `/target/compiled` directory.
- `dbt run` - it compiles the models and executes it against the warehouse. All runs are saved in `/target/run/` directory.
- `dbt clean` - it deletes the temporary files and the `/target` and other artifcats created during previous run processes.

### 5. Understand Project Structure

| Folder | Purpose |
| --- | --- |
| `models/` | SQL transformation files |
| `macros/` | Reusable Jinja logic |
| `tests/` | Data validation queries |
| `seeds/` | Seed CSVs |

### 6. Modular Architecture Pattern

Break the project into distinct layers:

```
models/
├── staging/        ← cleaned, standardized from raw sources
├── intermediate/   ← business logic transformations
└── marts/          ← final business-facing tables
```

### 7. Apply Jinja Templating

Three Jinja block types:

| Syntax | Purpose |
| --- | --- |
| `{{ ... }}` | Expressions / output |
| `{% ... %}` | Control logic (if/else, loops) |
| `{# ... #}` | Comments |

### 8. Configure Materializations

Control how models are built:

```sql
{{ config(materialized='table') }}       -- persisted snapshot
{{ config(materialized='view') }}        -- always queries source
{{ config(materialized='incremental') }} -- appends/updates new data
{{ config(materialized='ephemeral') }}   -- temporary or inline
```

### 9. dbt Tests in Schema Level (YAML format)
This is where you write standard tests like unique, not_null, relationships, and accepted_values.

```yaml
version: 2
models:
  - name: stg_orders
    description: "Staging table for orders data."
    columns:
      - name: order_id
        description: "Unique identifier for each order."
        tests:
          - unique
          - not_null
```
