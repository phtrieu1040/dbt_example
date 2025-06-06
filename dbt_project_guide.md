# dbt_project.yml Guide

A practical reference for configuring your dbt project, with examples, best practices, and ClickHouse-specific tips.

---

## Table of Contents
- [Summary Table of Useful Features](#summary-table-of-useful-features)
- [1. Project Metadata](#1-project-metadata)
- [2. Profile](#2-profile)
- [3. Paths](#3-paths)
- [4. Target and Clean](#4-target-and-clean)
- [5. Model Configurations (Scoping)](#5-model-configurations-scoping)
- [6. Advanced Features](#6-advanced-features)
- [7. ClickHouse-Specific Tips](#7-clickhouse-specific-tips)
- [8. Tags and Selectors Usage](#8-tags-and-selectors-usage)
- [9. Environment Management](#9-environment-management)
- [10. Documentation and dbt Docs](#10-documentation-and-dbt-docs)
- [11. Best Practices](#11-best-practices)
- [12. Troubleshooting & FAQ](#12-troubleshooting--faq)
- [13. Example Directory Structure](#13-example-directory-structure)
- [14. .gitignore](#14-gitignore)
- [ClickHouse Table Engines: Overview and Best Practices](#clickhouse-table-engines-overview-and-best-practices)
- [Using vars and args: Models vs Macros](#using-vars-and-args-models-vs-macros)

---

## Summary Table of Useful Features

| Feature         | What it does                                         | Example Usage/Value                        |
|-----------------|------------------------------------------------------|--------------------------------------------|
| +materialized   | Table, view, incremental, ephemeral                  | `+materialized: table`                     |
| +schema         | Controls schema/database                             | `+schema: analytics`                       |
| +tags           | Group models for selective runs/tests                | `+tags: ['core', 'finance']`               |
| +description    | Adds docs for models/columns                         | `+description: "User fact table"`         |
| +alias          | Custom table/view name                               | `+alias: my_custom_table`                  |
| +post-hook      | Run SQL after model build (e.g., optimize)           | `+post-hook: "OPTIMIZE TABLE {{ this }} FINAL"` |
| +pre-hook       | Run SQL before model build                           | `+pre-hook: "SET some_setting = 1"`       |
| +tests          | Data quality checks (in YAML)                        | `tests: [not_null, unique]` in YAML        |
| +vars           | Project or environment variables                     | `vars: {run_type: 'prod'}`                 |
| +docs-paths     | Markdown docs for dbt docs site                      | `docs-paths: ["docs"]`                    |
| +macro-paths    | Reusable SQL/Jinja logic                             | `macro-paths: ["macros"]`                 |
| +seed-paths     | Load CSVs as tables                                  | `seed-paths: ["seeds"]`                   |
| +snapshot-paths | Track SCDs (slowly changing dimensions)              | `snapshot-paths: ["snapshots"]`           |
| +analysis-paths | Ad-hoc analysis queries (not built by default)       | `analysis-paths: ["analysis"]`            |
| +test-paths     | Custom data tests (SQL files)                        | `test-paths: ["tests"]`                   |
| threads         | Set number of parallel threads for dbt runs           | `threads: 8`                               |
| on-run-start/on-run-end | Run SQL or macros at the start/end of a dbt run | `on-run-start: ["{{ log('Start', info=True) }}"]` |
| env_var         | Use environment variables in configs                  | `host: "{{ env_var('CLICKHOUSE_HOST') }}"`|
| selectors       | Define groups of models for selective runs            | `selectors: [my_selector.yml]`             |
| seeds           | Reference/test data as tables                         | `dbt seed`                                 |
| snapshots       | Track changes over time (SCDs)                        | `dbt snapshot`                             |

---

## 1. Project Metadata
```yaml
name: my_project
version: '1.0'
config-version: 2
```
- **name**: Project name (should match your folder).
- **version**: Arbitrary project version.
- **config-version**: Always 2 for modern dbt.

---

## 2. Profile
```yaml
profile: my_profile
```
- **profile**: Name of the connection profile in `profiles.yml`.

---

## 3. Paths
```yaml
model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
seed-paths: ["seeds"]
docs-paths: ["docs"]
```
- Organize your project for clarity and scalability.

---

## 4. Target and Clean
```yaml
target-path: target
clean-targets:
  - target
  - dbt_modules
```
- **target-path**: Where dbt writes compiled SQL and artifacts.
- **clean-targets**: Folders deleted by `dbt clean`.

---

## 5. Model Configurations (Scoping)
```yaml
models:
  my_project:
    staging:
      +materialized: view
      +tags: ['staging']
      +description: "Staging models for raw data."
    marts:
      +materialized: table
      +schema: analytics
      +tags: ['production']
      +post-hook: "OPTIMIZE TABLE {{ this }} FINAL"
      +description: "Production-ready marts."
      +alias: my_custom_mart
```
- Set configs for all models, folders, or individual models.
- Use folder-level configs to avoid repetition.
- Override in a model file only for exceptions.

---

## 6. Advanced Features (Full Example)

Below is a full example `dbt_project.yml` that includes all the features from the summary table:

```yaml
name: my_project
version: '1.0'
config-version: 2
profile: my_profile

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
seed-paths: ["seeds"]
docs-paths: ["docs"]
target-path: target
clean-targets:
  - target
  - dbt_modules

vars:
  run_type: 'prod'
  my_var: 'value'
threads: 8
on-run-start:
  - "{{ log('Starting dbt run', info=True) }}"
on-run-end:
  - "{{ log('Finished dbt run', info=True) }}"

selectors:
  - name: my_selector
    definition:
      method: tag
      value: core

models:
  my_project:
    staging:
      +materialized: view
      +tags: ['staging']
      +description: "Staging models for raw data."
      +pre-hook: "SET some_setting = 1"
      +tests:
        - not_null
        - unique
    marts:
      +materialized: table
      +schema: analytics
      +tags: ['production']
      +post-hook: "OPTIMIZE TABLE {{ this }} FINAL"
      +description: "Production-ready marts."
      +alias: my_custom_mart
      +docs-paths: ["docs"]
      +macro-paths: ["macros"]
      +seed-paths: ["seeds"]
      +snapshot-paths: ["snapshots"]
      +analysis-paths: ["analysis"]
      +test-paths: ["tests"]
      +vars:
        mart_var: 'mart_value'
```

This example covers:
- All path settings
- All model-level configs (materialized, schema, tags, description, alias, hooks, tests, vars)
- Threading, vars, on-run hooks, selectors
- ClickHouse-specific post-hook for optimization
- Environment variable usage (see `profiles.yml` for `env_var`)

---

## 7. ClickHouse-Specific Tips
- Use `engine='ReplacingMergeTree()'` for upsert-like behavior.
- Use `OPTIMIZE TABLE {{ this }} FINAL` as a post-hook for immediate deduplication.
- ClickHouse does not have schemas like Postgres/Snowflake; dbt emulates schemas by prefixing table names.
- Use partitioning and order_by for large tables for better performance.
- Monitor merges and use manual `OPTIMIZE` as needed for large/critical tables.

---

## 8. Tags and Selectors Usage
- **Tags**: Add `+tags` in dbt_project.yml or model files to group models.
- **Selectors**: Use selector YAML files for complex selection logic.

**Example:**
```yaml
+tags: ['core', 'finance']
```
Run only tagged models:
```sh
dbt run --select tag:core
dbt test --select tag:finance
```

---

## 9. Environment Management
- Use `profiles.yml` for different database connections (dev, prod, etc.).
- Use `env_var` in `profiles.yml` and `dbt_project.yml` for secrets and environment-specific configs.
- Use `vars` for environment-specific logic in models/macros.

**Example:**
```yaml
host: "{{ env_var('CLICKHOUSE_HOST') }}"
```

---

## 10. Documentation and dbt Docs
- Add `+description` in YAML for models/columns.
- Use `docs()` blocks in model SQL for rich documentation.
- Store markdown docs in `docs/`.
- Generate docs: `dbt docs generate`
- Serve docs: `dbt docs serve`

---

## 11. Best Practices
- Use folder-level configs for DRY (Don't Repeat Yourself) setup.
- Use tags/selectors for targeted runs and tests.
- Use post-hooks for ClickHouse optimization if needed.
- Organize your project with clear folder structure.
- Document your models and columns for dbt docs.
- Use environment variables for secrets/configs.
- Use seeds for reference/test data.
- Use snapshots for slowly changing dimensions.
- Use macros for reusable SQL logic.

---

## 12. Troubleshooting & FAQ
- **Q: My model is not materialized as a table, why?**
  - A: Check for `materialized` config in the model file, folder, and dbt_project.yml. Model file config takes precedence.
- **Q: Why is my table name prefixed with the schema?**
  - A: In ClickHouse, dbt emulates schemas by prefixing the table name with the schema name.
- **Q: Why do I see duplicate rows in ReplacingMergeTree?**
  - A: Deduplication happens after merges. Use `OPTIMIZE TABLE ... FINAL` for immediate effect.
- **Q: How do I run only a subset of models?**
  - A: Use tags or selectors: `dbt run --select tag:mytag` or a selector YAML file.

---

## 13. Example Directory Structure
```
models/
  staging/
    my_staging_model.sql
  marts/
    my_mart_model.sql
analysis/
tests/
macros/
snapshots/
seeds/
docs/
```

---

## 14. .gitignore
Add this guide to your `.gitignore` to avoid committing it if it's for internal reference:
```
dbt_project_guide.md
```

## ClickHouse Table Engines: Overview and Best Practices

ClickHouse supports a variety of table engines, each designed for specific use cases. Choosing the right engine is crucial for performance, data integrity, and maintainability in your dbt project.

### 1. MergeTree Family (Most Common for Analytics)

- **MergeTree**: The base engine for analytics. No deduplication; stores all rows. Use for general analytics when you want to keep all data.
- **ReplacingMergeTree**: Deduplicates rows with the same primary key (optionally by a version column) during background merges. Use for upserts, CDC, or keeping only the latest version of a row.
- **SummingMergeTree**: Sums numeric columns for rows with the same primary key during merges. Use for aggregated fact tables (e.g., daily sales totals).
- **AggregatingMergeTree**: Stores aggregate states and merges them. Use for advanced pre-aggregated data/materialized views.
- **CollapsingMergeTree**: Supports row versioning with a sign column (+1/-1), collapses rows during merges. Use for event sourcing, soft deletes, or undo/redo logic.
- **VersionedCollapsingMergeTree**: Like CollapsingMergeTree, but with a version column for more precise control.
- **GraphiteMergeTree**: Special engine for time-series data with retention policies. Use for storing metrics with automatic downsampling.

### 2. Log Engines (For Simple, Fast Inserts)

- **Log**: Simple, no indexes or partitions. Use for small tables, staging, or logs.
- **TinyLog**: Even simpler, stores all data in a single file. Use for very small tables or testing.
- **StripeLog**: Like Log, but stores data in stripes (blocks). Use for small tables, better for parallel reads.

### 3. External/Integration Engines

- **Memory**: Stores all data in RAM. Use for temporary or very fast lookup tables (data lost on restart).
- **Null**: Discards all data written to it. Use for testing or debugging.
- **File**: Reads/writes data from/to a file (CSV, TSV, etc.). Use for import/export or external data.
- **Kafka**: Reads data from Kafka topics. Use for streaming ingestion.
- **MySQL, PostgreSQL, ODBC, JDBC**: Reads data from external databases. Use for federated queries or data integration.

### 4. Special Engines

- **Merge**: Virtual engine to query multiple tables as one. Use for querying sharded tables.
- **Distributed**: For distributed clusters, routes queries to shards/replicas. Use for scaling out ClickHouse.

### Summary Table

| Engine                        | Use Case / Description                                 | Common? |
|-------------------------------|-------------------------------------------------------|---------|
| MergeTree                     | General analytics, all data kept                      | ⭐⭐⭐⭐⭐   |
| ReplacingMergeTree            | Upserts, deduplication                                | ⭐⭐⭐⭐    |
| SummingMergeTree              | Pre-aggregated numeric data                           | ⭐⭐⭐     |
| AggregatingMergeTree          | Advanced pre-aggregation                              | ⭐⭐      |
| CollapsingMergeTree           | Event sourcing, soft deletes                          | ⭐⭐      |
| VersionedCollapsingMergeTree  | Advanced event sourcing                               | ⭐       |
| GraphiteMergeTree             | Time-series with retention                            | ⭐       |
| Log, TinyLog, StripeLog       | Small, simple, or staging tables                      | ⭐⭐      |
| Memory                        | Fast, temporary, in-memory tables                     | ⭐⭐      |
| Null                          | Discard data, testing                                 | ⭐       |
| File                          | Import/export, external files                         | ⭐       |
| Kafka                         | Streaming ingestion                                   | ⭐       |
| MySQL, PostgreSQL, ODBC, JDBC | External DB integration                               | ⭐       |
| Merge                         | Query multiple tables as one                          | ⭐       |
| Distributed                   | Distributed clusters                                  | ⭐⭐⭐     |

### Best Practices for dbt + ClickHouse Engine Selection

- **Default to `MergeTree` or `ReplacingMergeTree`** for most analytics tables.
- **Use `ReplacingMergeTree`** if you need upserts or deduplication by primary key (optionally with a version column).
- **Use `SummingMergeTree` or `AggregatingMergeTree`** for pre-aggregated or summary tables.
- **Use `Log` or `TinyLog`** for small, staging, or temporary tables.
- **Partitioning:** Partition by month or year (e.g., `toStartOfMonth(date_key)`) for large tables to avoid too many partitions per insert.
- **Override engine and partitioning in model files** if a specific table has unique requirements; otherwise, set sensible defaults in `dbt_project.yml`.
- **Distributed engine** is needed for sharded/replicated clusters.

**Choose the engine based on your data's lifecycle, deduplication, aggregation, and performance needs.** 

---

## Using vars and args: Models vs Macros

**dbt models** use `vars`, while **dbt macros** (run with `dbt run-operation`) use `args`.

### 1. Using `vars` with Models
- Pass variables to models with `--vars`.
- Access in SQL with `var('my_var')`.

**Example: models/example_model.sql**
```sql
select '{{ var("run_type") }}' as run_type
```
**Run:**
```sh
dbt run --vars '{"run_type": "prod"}'
```

### 2. Using `args` with Macros
- Pass arguments to macros with `--args`.
- Define macro parameters in your macro.

**Example: macros/example_macro.sql**
```jinja
{% macro print_arg(my_arg) %}
    {{ log('my_arg is: ' ~ my_arg, info=True) }}
{% endmacro %}
```
**Run:**
```sh
dbt run-operation print_arg --args '{"my_arg": "hello"}'
```

### 3. Python Example (Prefect)
**Run a macro with args in Python:**
```python
from prefect_dbt.cli.commands import DbtCoreOperation
DbtCoreOperation(
    commands=["dbt run-operation print_arg --args '{\"my_arg\": \"hello\"}'"],
    project_dir="my_dbt_project",
    profiles_dir="my_dbt_project"
).run()
```

**Summary:**
- Use `--vars` for models (`var('...')` in SQL).
- Use `--args` for macros (`macro_name(arg1, arg2, ...)`).
- Macro names must be unique; file names do not matter. 