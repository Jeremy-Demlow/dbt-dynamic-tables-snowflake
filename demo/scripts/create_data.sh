# Create the initial test data
dbt run-operation generate_test_orders --args '{days_back: 30, orders_per_day: 100, drop_existing: true}'
