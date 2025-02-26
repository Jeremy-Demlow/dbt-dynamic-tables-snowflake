# Snowflake Dynamic Tables with dbt

This dbt project demonstrates how to implement and effectively use Snowflake Dynamic Tables, a powerful feature that combines the convenience of materialized views with the performance of tables.

## What are Dynamic Tables?

Dynamic tables are a Snowflake-specific feature that:

- Automatically refresh on a schedule you define
- Support incremental refreshes for efficiency
- Allow you to use complex SQL functionality not supported by materialized views
- Can be queried like any other table

## Project Architecture

```
demo/
├── analyses/            # Analysis SQL files for monitoring
├── macros/              # Helper macros and data generation utilities
├── models/              # dbt models
│   ├── marts/           # Business-level models
│   │   ├── core/        # Primary dynamic table models
│   │   └── integrations/# Cross-domain models
│   ├── sources.yml      # Source definitions
│   └── staging/         # Source cleaning models
├── scripts/             # Utility scripts for setup and testing
├── seeds/               # Reference data (empty in this project)
├── dbt_project.yml      # Project configuration
└── packages.yml         # External package dependencies
```

## Dynamic Table Configuration

This project implements dynamic tables with standardized configuration using dbt's hierarchical config system:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `target_lag` | How often the table refreshes | `'1 hour'` |
| `snowflake_warehouse` | Warehouse used for refreshes | `'dbt_wh_xs'` |
| `refresh_mode` | AUTO, FULL, or INCREMENTAL | `'INCREMENTAL'` |
| `initialize` | ON_CREATE or ON_SCHEDULE | `'ON_CREATE'` |
| `on_configuration_change` | apply, continue, or fail | `'apply'` |

### Configuration Best Practices

The project uses variables and hierarchical config to standardize dynamic table settings:

```yaml
# In dbt_project.yml
vars:
  dynamic_table_refresh:
    target_lag: '1 hour'
    warehouse: 'dbt_wh_xs'
    refresh_mode: 'INCREMENTAL'
    initialize: 'ON_CREATE'
    configuration_change: 'apply'

models:
  my_dynamic_project:
    +dynamic_table_defaults:
      +materialized: dynamic_table
      +target_lag: "{{ var('dynamic_table_refresh').target_lag }}"
      # additional settings...
```

Models can then inherit these settings:

```sql
-- In model SQL files
{{ config(materialized='dynamic_table') }}

-- SQL logic follows, no need to repeat configurations
```

## Example Models

### Daily Sales Aggregation

The `daily_sales` model demonstrates a dynamic table that aggregates sales data by date and region with automatic hourly refresh:

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
),
-- additional CTEs...

select
    order_date,
    region_name,
    country,
    order_count,
    -- additional metrics...
from daily_sales
```

### Cross-Domain Customer View

The `customer_360_view` model shows how to combine sales and marketing data in an enterprise-wide view:

```sql
with sales as (
    select * from {{ ref('daily_customer_sales') }}
),
marketing as (
    select * from {{ ref('stg_marketing_events') }}
),
-- additional CTEs...

select
    customer_id,
    activity_date,
    -- additional fields...
from daily_customer_activity
```

## Setup Instructions

### Prerequisites

- Snowflake account with appropriate permissions
- dbt CLI installed
- Snowflake account with `QUOTED_IDENTIFIERS_IGNORE_CASE` set to FALSE

### Setup Steps

1. Clone this repository
2. Set up your connection profile:

```yaml
# profiles.yml
my_dynamic_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: [your-account]
      user: [your-username]
      password: [your-password]
      role: [your-role]
      database: SALES_DB
      warehouse: compute_wh
      schema: dbt_dev
      threads: 4
      client_session_keep_alive: True
```

3. Install dependencies:
```bash
dbt deps
```

4. Create test data:
```bash
dbt run-operation generate_test_orders --args '{days_back: 30, orders_per_day: 100, drop_existing: true}'
dbt run-operation generate_marketing_data --args '{days_back: 90, events_per_day: 200, drop_existing: true}'
```

5. Build models:
```bash
dbt build
```

## Testing Dynamic Table Refresh

1. Generate initial test data:
```bash
./scripts/create_data.sh
```

2. Build the models:
```bash
dbt build
```

3. Add incremental data:
```bash
./scripts/add_incremental_data.sh
```
## Useful Operations

| Command | Description |
|---------|-------------|
| `refresh_dynamic_table` | Force an immediate refresh of a dynamic table |
| `get_dynamic_table_refresh_history` | View refresh history for a dynamic table |
| `add_incremental_test_data` | Add new test data for refresh testing |

## Limitations and Troubleshooting

### Dynamic Table Limitations

- Dynamic table SQL has a limited feature set compared to regular tables
- SQL cannot be updated without a full-refresh (DROP/CREATE)
- Cannot be downstream from: materialized views, external tables, streams
- Cannot reference a view that references another dynamic table


### Common Issues

**Failure to update configuration:**
```
SnowflakeDynamicTableConfig.__init__() missing 6 required positional arguments
```
Solution: Ensure `QUOTED_IDENTIFIERS_IGNORE_CASE` is set to FALSE on your account.

**Refresh failures:**
- Check warehouse sizing for compute-intensive operations
- Verify SQL is compatible with incremental refresh
- Review refresh history for specific error messages


## Contributing

Contributions are welcome! Please submit PRs with:

- Additional test utilities
- Enhanced monitoring scripts
- Documentation improvements
- New dynamic table examples