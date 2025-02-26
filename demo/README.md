# My Dynamic Project

This dbt project demonstrates the use of Snowflake Dynamic Tables with dbt.

## Project Structure

- **models/staging/** - Contains staging models that clean and prepare source data
- **models/marts/core/** - Contains business-level aggregations, including our dynamic table
- **seeds/** - Contains reference data loaded via dbt seed
- **macros/** - Contains custom macros for working with dynamic tables

## Dynamic Tables

This project includes an example dynamic table (`daily_sales`) that aggregates order data by date and region. The dynamic table is configured to refresh every hour using Snowflake's automatic refresh capability.

### Configuration

The dynamic table is configured with the following parameters:

- **target_lag**: 1 hour (refreshes every hour)
- **snowflake_warehouse**: compute_wh (the warehouse used for refreshes)
- **refresh_mode**: AUTO (uses incremental refresh when possible)
- **initialize**: ON_CREATE (populates the table immediately when created)
- **on_configuration_change**: apply (applies configuration changes immediately)

### Useful Commands

```bash
# Build the project
dbt build

# Run just the dynamic table model
dbt run --select daily_sales

# Rebuild the dynamic table from scratch
dbt run --select daily_sales --full-refresh

# Check dynamic table refresh history
dbt run-operation get_dynamic_table_refresh_history --args "{model_name: 'daily_sales'}"

# Force a refresh of the dynamic table
dbt run-operation refresh_dynamic_table --args "{model_name: 'daily_sales'}"
```

## Setup Instructions

1. Clone this repository
2. Set up your Snowflake connection profile:
   - Either create a `profiles.yml` file in the project directory (for local development)
   - Or update your global `~/.dbt/profiles.yml` file
   - Use the following template, replacing the placeholders with your actual Snowflake credentials:

```yaml
my_dynamic_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: [your-account]     # e.g., xy12345.us-east-1
      user: [your-username]       # Your Snowflake username
      password: [your-password]   # Your password or use key-pair auth
      role: [your-role]           # e.g., ACCOUNTADMIN, SYSADMIN, etc.
      database: analytics         # The database to use
      warehouse: compute_wh       # The warehouse to use
      schema: dbt_dev             # The schema to use
      threads: 4
      client_session_keep_alive: True
```

3. Run `dbt deps` to install dependencies
4. Run `dbt seed` to load reference data
5. Run `dbt build` to build the entire project

## Testing Incremental Refresh Capability

This project includes tools to generate test data and validate the incremental refresh capabilities of dynamic tables:

### Generate Test Data

```bash
# Generate 30 days of test order data (3000 orders)
dbt run-operation generate_test_orders --args "{\"days_back\": 30, \"orders_per_day\": 100, \"drop_existing\": true}"

# Build models including the dynamic table
dbt build

# Add new incremental data (2 more days, 50 orders per day)
dbt run-operation add_incremental_test_data --args "{\"days\": 2, \"orders_per_day\": 50}"

# Force a refresh of the dynamic table
dbt run-operation refresh_dynamic_table --args "{\"model_name\": \"daily_sales\"}"

# Validate the refresh was successful
dbt run-operation validate_dynamic_table_refresh --args "{\"model_name\": \"daily_sales\"}"
```

### Automated Testing Script

For convenience, you can run the entire test sequence with:

```bash
./scripts/setup_test_environment.sh
```

### Analyzing Refresh Patterns

To get deeper insights into how your dynamic table is refreshing:

```bash
# Run the analysis SQL
dbt compile --select dynamic_table_refresh_analysis
dbt run-operation run_query --args "{\"query\": \"$(cat target/compiled/my_dynamic_project/analyses/dynamic_table_refresh_analysis.sql)\"}"
```

## Snowflake Account Configuration

Ensure your Snowflake account has:
- `QUOTED_IDENTIFIERS_IGNORE_CASE` set to FALSE
- Sufficient privileges for creating and managing dynamic tables


# Manual Incremental Testing Instructions

If you prefer to test incremental refreshes without using the automated scripts, you can follow these manual steps:

## 1. Set Up Your Environment

First, ensure your profiles.yml is correctly set up:

```bash
# Check if your configuration is valid
dbt debug
```

## 2. Install Dependencies

```bash
dbt deps
```

## 3. Create Raw Schema and Generate Test Data

```bash
# Create raw database and schema
dbt run-operation run_query --args '{"query": "create database if not exists raw"}'
dbt run-operation run_query --args '{"query": "create schema if not exists raw.sales"}'

# Generate test order data going back 30 days (3000 orders)
dbt run-operation generate_test_orders --args '{"days_back": 30, "orders_per_day": 100, "drop_existing": true}'
```

## 4. Build the Initial Project

```bash
# Build the project with the dynamic table
dbt build
```

## 5. Check Initial Dynamic Table Status

```bash
# Check the refresh history
dbt run-operation get_dynamic_table_refresh_history --args '{"model_name": "daily_sales"}'
```

## 6. Add Incremental Data

```bash
# Add 2 more days of data (100 new orders)
dbt run-operation add_incremental_test_data --args '{"days": 2, "orders_per_day": 50}'
```

## 7. Force a Refresh (Optional)

If you don't want to wait for the automatic refresh based on target_lag:

```bash
# Force a refresh of the dynamic table
dbt run-operation refresh_dynamic_table --args '{"model_name": "daily_sales"}'
```

## 8. Validate the Refresh

```bash
# Check if the refresh worked correctly
dbt run-operation validate_dynamic_table_refresh --args '{"model_name": "daily_sales"}'
```

## 9. Analyze Refresh Patterns

```bash
# Run the dynamic table monitor analysis
dbt compile --select dynamic_table_monitor
dbt run-operation run_query --args "{\"query\": \"$(cat target/compiled/my_dynamic_project/analyses/dynamic_table_monitor.sql)\"}"
```

## Troubleshooting

If the dynamic table is not refreshing as expected:

1. Check if the SQL query meets Snowflake's requirements for incremental refreshes
2. Verify the warehouse is properly sized for the refresh operations
3. Look at the refresh history for any error messages
4. Ensure your Snowflake account has QUOTED_IDENTIFIERS_IGNORE_CASE set to FALSE