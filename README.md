# Go ETL Pipeline Builder

Un framework ETL modular scris în Go pentru definirea si rularea pipeline-urilor de tip Extract → Transform → Load folosind fisiere YAML de configurare.

Scop

- Permite extragerea datelor din surse variate (CSV, JSON, API, PostgreSQL), aplicarea unor transformari (mapari, filtre, agregari) si încarcarea rezultatelor în destinatii diverse (CSV, SQLite, PostgreSQL, stdout).

Caracteristici principale

- Extractoare incluse: CSV, JSON, API, PostgreSQL
- Transformari: `map` (re-mapping coloane), `filter` (expresii compacte), `aggregate` (group-by + operatii)
- Loaders: `stdout`, `csv`, `sqlite`, `postgres`
- Programare (scheduling) cu interval si retry
- Validare rânduri pe baza regulilor declarative din YAML
- Monitorizare minimala: server HTTP pentru metrici si istoric de executii

Arhitectura proiect

- `cmd/etl/main.go` — punctul de intrare; încarca configuratia, porneste monitorul si declanseaza executiile programate.
- `internal/config` — parsare YAML si structuri pentru `PipelineConfig`.
- `internal/connector` — extractoare pentru CSV, JSON, API, PostgreSQL.
- `internal/transform` — implementari pentru mapari, filtre si agregari.
- `internal/load` — scrierea rezultatelor catre CSV, SQLite, PostgreSQL sau stdout.
- `internal/runner` — coordoneaza executia: validare, extract → transform\* → load.
- `internal/logger` — initializare si wrapper pentru logare.
- `internal/monitor` — server HTTP simplu si înregistrare a rularilor.
- `examples/` — fisiere de exemple YAML, date de test si output-uri demonstrative.

Instalare si cerinte

- Necesita Go (versiunea folosita în modulul proiectului: 1.25.4). Vezi `go.mod` pentru detalii.

Pasi rapizi

1. Obtine dependentele:

```bash
go mod download
```

2. Rulare în modul dezvoltare (fara build):

```bash
go run ./cmd/etl
```

3. Sau build + rulare:

```bash
go build -o etl ./cmd/etl
./etl
```

Configurare pipeline (YAML)

- Configuratia unui pipeline se afla în fisiere YAML din `examples/` (ex: `examples/pipeline.yml`, `examples/pipeline_postgres.yml`). Structura principala include `name`, `extract`, `transform`, `load`, `schedule` si, optional, `validate`.
- Exemple de proprietati:
  - `extract.type`: `csv|json|api|postgres`
  - `extract.config`: mapa generica cu parametrii specifici (ex: `path` pentru CSV, `url` pentru API, `conn`+`query` pentru Postgres)
  - `transform`: lista de pasi cu `type` = `map|filter|aggregate`
  - `load.type`: `stdout|csv|sqlite|postgres`
  - `load.config`: parametrii loader-ului (ex: `path` pentru CSV, `conn`+`table` pentru Postgres)
  - `schedule.interval_seconds` si `schedule.retries` pentru rulare repetata

Exemple

- Vezi folderul `examples/` pentru pipeline-uri gata de folosit si date test (ex: `examples/pipeline.yml`, `examples/sales.csv`, `examples/pipeline_postgres.yml`).

Comportament runtime

- `cmd/etl/main.go` încarca configuratia cu `config.LoadPipelineConfig(path)` (ex: `./examples/pipeline.yml`), porneste serverul de monitorizare pe portul 8080 si ruleaza pipeline-ul la intervalul definit. Executiile sunt încercate cu un mecanism de retry si raporteaza succes/eroare în monitor si log-uri.

Extindere

- Adaugarea unui nou connector: implementati functia de extractie în `internal/connector` si extindeti `runner.Run` pentru a apela noul tip.
- Adaugarea unui loader: creati functia în `internal/load` si adaugati tipul în `runner.Run`.

Contributii

- Fork & pull request. Descrieti schimbarea si includeti exemple/config actualizate.

Fisiere utile

- `cmd/etl/main.go` — entrypoint si scheduler
- `internal/config/configuration.go` — structura `PipelineConfig` si functia `LoadPipelineConfig`
- `internal/runner/runner.go` — orchestratorul principal al pipeline-ului

# Go ETL Pipeline Builder

Un framework ETL modular scris in Go pentru definirea si rularea pipeline-urilor de tip Extract -> Transform -> Load folosind fisiere YAML de configurare.

Overview

- Build a flexible Extract-Transform-Load system that moves data between different sources (databases, APIs, files) while transforming it according to configurable rules.

Why This Project?

- ETL is fundamental to data engineering. You'll learn about data pipelines, transformation logic, error handling at scale, and building systems that reliably move data.

User Stories

- As a data engineer, I can define pipelines connecting different data sources
- As a data engineer, I can apply transformations (map fields, filter, aggregate)
- As a data engineer, I can schedule pipelines to run automatically
- As a data engineer, I can monitor pipeline execution and catch failures
- As a data engineer, I can validate data quality with custom rules

Technical Requirements

- Pipeline configuration (YAML or visual builder)
- Multiple data source connectors (databases, APIs, files)
- Transformation operations (field mapping, filtering, aggregations)
- Scheduling and orchestration
- Error handling and retries
- Data validation rules
- Monitoring dashboard

Features

- Extractors included: CSV, JSON, API, PostgreSQL
- Transformations: map (re-mapping fields), filter (row-level expressions), aggregate (group-by + operations)
- Loaders: stdout, csv, sqlite, postgres
- Scheduling with interval and retries
- Declarative row validation via YAML
- Minimal monitoring server that exposes metrics and run history

Project architecture

- `cmd/etl/main.go` - entrypoint; loads pipeline config, starts monitor and triggers scheduled runs
- `internal/config` - YAML parsing and `PipelineConfig` structs
- `internal/connector` - extractors: CSV, JSON, API, Postgres
- `internal/transform` - mapping, filtering, aggregation logic
- `internal/load` - CSV, SQLite, Postgres, stdout loaders
- `internal/runner` - orchestrator: validate -> extract -> transform -> load
- `internal/logger` - logging initialization and helpers
- `internal/monitor` - simple HTTP server and run history
- `examples/` - sample pipelines, test data and outputs

Install and requirements

- Requires Go (see `go.mod` for the exact version and deps).

Quick start

1. Download dependencies:

```bash
go mod download
```

2. Run in development mode:

```bash
go run ./cmd/etl
```

3. Or build and run:

```bash
go build -o etl ./cmd/etl
./etl
```

Pipeline configuration (YAML)

- Pipeline configs are in `examples/` (e.g. `examples/pipeline.yml`). Main fields: `name`, `extract`, `transform`, `load`, `schedule`, optional `validate`.
- Example fields:
  - `extract.type`: `csv|json|api|postgres`
  - `extract.config`: generic map with extractor params (`path` for CSV, `url` for API, `conn`+`query` for Postgres)
  - `transform`: list of steps with `type` = `map|filter|aggregate`
  - `load.type`: `stdout|csv|sqlite|postgres`
  - `load.config`: loader params (`path` for CSV, `conn`+`table` for Postgres)
  - `schedule.interval_seconds` and `schedule.retries`

Examples

- See `examples/` for ready-to-run pipeline YAMLs and sample data (`examples/pipeline.yml`, `examples/sales.csv`, `examples/pipeline_postgres.yml`).

Runtime behavior

- `cmd/etl/main.go` loads the pipeline with `config.LoadPipelineConfig(path)`, starts the monitor HTTP server on port 8080 and executes the pipeline on the configured schedule. Runs use a retry mechanism and report status to monitor and logs.

Extending the project

- Add a connector: implement extractor in `internal/connector` and update `runner.Run` to support the new type.
- Add a loader: implement loader in `internal/load` and add the type in `runner.Run`.

Contributing

- Fork, implement changes, update examples/configs and open a pull request describing your changes.

Useful files

- `cmd/etl/main.go` - entrypoint and scheduler
- `internal/config/configuration.go` - `PipelineConfig` and `LoadPipelineConfig`
- `internal/runner/runner.go` - main orchestrator
