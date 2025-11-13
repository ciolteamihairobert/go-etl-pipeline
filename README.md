Go ETL Pipeline Builder is a modular Extract–Transform–Load (ETL) system written in Go.
Its purpose is to provide a configurable framework for moving and transforming data between multiple sources such as CSV files, APIs, and databases.
The system is driven by a YAML configuration file that defines each pipeline’s extract, transform, and load stages.

Current State (Checkpoint 1)
At this stage, the project includes:
Project structure following Go conventions (cmd/, internal/, examples/, docs/).
Configuration system that loads and parses YAML pipeline definitions (internal/config).
CSV extractor that reads a CSV file and prints its content (internal/connector).
Working example consisting of:
A pipeline definition in YAML (examples/sales_pipeline.yml).
A sample CSV dataset (examples/sales.csv).
Executable entry point (cmd/etl/main.go) that loads the configuration and triggers extraction.
The application can successfully load a YAML configuration and print the CSV file’s rows to the console.

| Stage          | Feature               | Description                                  |
| -------------- | --------------------- | -------------------------------------------- |
| **Extract**    | Multiple connectors   | Read data from CSV, databases, and APIs      |
| **Transform**  | Field mapping         | Rename and map fields dynamically            |
|                | Filtering             | Include or exclude rows based on expressions |
|                | Aggregations          | Compute totals, averages, counts, etc.       |
| **Load**       | Multiple destinations | Write data to files (CSV/JSON) or databases  |
| **Scheduling** | Automatic execution   | Run pipelines at scheduled intervals         |
| **Validation** | Data quality rules    | Check for missing or invalid values          |
| **Monitoring** | Dashboard             | Log and track pipeline executions            |
