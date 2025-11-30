package runner

import (
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/connector"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/load"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/transform"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/validation"
)

func Run(cfg *config.PipelineConfig) error {
	logger.Info.Println("Validating pipeline configuration...") // logam un mesaj de validare a configuratiei

	if err := cfg.Validate(); err != nil { // validam configuratia pipeline-ului
		logger.Error.Printf("Validation failed: %v", err)        // logam eroarea de validare
		return fmt.Errorf("pipeline validation failed: %w", err) // returnam eroarea
	}

	logger.Info.Println("Starting extraction...") // logam un mesaj de start al extractiei
	var header []string                           // slice pentru header
	var rows [][]string                           // slice pentru randuri
	var err error                                 // variabila pentru eroare

	switch cfg.Extract.Type { // comutam in functie de tipul de extractie
	case "csv": // daca tipul este csv
		header, rows, err = connector.ExtractCSV(cfg.Extract.Config) // extragem datele din CSV

	case "json": // daca tipul este json
		header, rows, err = connector.ExtractJSON(cfg.Extract.Config) // extragem datele din JSON

	case "api": // daca tipul este api
		header, rows, err = connector.ExtractAPI(cfg.Extract.Config) // extragem datele din API

	case "postgres": // daca tipul este postgres
		header, rows, err = connector.ExtractPostgres(cfg.Extract.Config) // extragem datele din Postgres

	default: // pentru tipuri necunoscute
		return fmt.Errorf("unknown extract type: %s", cfg.Extract.Type) // returnam eroarea
	}

	if err != nil { // daca a aparut o eroare la extractie
		logger.Error.Printf("Extract failed: %v", err) // logam eroarea
		return err                                     // returnam eroarea
	}

	logger.Info.Printf("Extracted %d rows", len(rows)) // logam numarul de randuri extrase

	if len(cfg.DataValidation) > 0 { // daca exista reguli de validare a datelor
		logger.Info.Printf("Validating %d rows with %d rules...", len(rows), len(cfg.DataValidation)) // logam un mesaj de validare
		for i, r := range rows {                                                                      // iteram prin randuri
			if err := validation.ValidateRow(header, r, cfg.DataValidation); err != nil { // validam randul curent
				logger.Error.Printf("Row %d failed validation: %v", i, err) // logam eroarea de validare
				return err                                                  // returnam eroarea
			}
		}
		logger.Info.Println("All rows passed validation!") // logam un mesaj de succes
	}

	for _, step := range cfg.Transform { // iteram prin pasii de transformare
		logger.Info.Printf("Applying transform step: %s", step.Type) // logam tipul pasului de transformare

		switch step.Type { // comutam in functie de tipul pasului
		case "filter": // daca tipul este filter
			rows = transform.ApplyFilter(rows, header, step.Expression)         // aplicam filtrul
			logger.Info.Printf("Filter applied. Remaining rows: %d", len(rows)) // logam numarul de randuri ramase

		case "map": // daca tipul este map
			header, rows = transform.ApplyMapping(rows, header, step.Mapping) // aplicam maparea
			logger.Info.Println("Mapping applied.")                           // logam un mesaj de succes

		case "aggregate": // daca tipul este aggregate
			header, rows = transform.Aggregate(rows, header, step.GroupBy, step.Operations) // aplicam agregarea
			logger.Info.Println("Aggregation applied.")                                     // logam un mesaj de succes
		}
	}

	logger.Info.Printf("Starting load operation (%s)...", cfg.Load.Type) // logam un mesaj de start al incarcarii

	switch cfg.Load.Type { // comutam in functie de tipul de load
	case "stdout": // daca tipul este stdout
		return load.ToStdout(header, rows) // incarcam datele in stdout

	case "sqlite": // daca tipul este sqlite
		return load.ToSQLite(cfg.Load.Config, header, rows) // incarcam datele in baza de date sqlite

	default: // pentru tipuri necunoscute
		logger.Error.Printf("Unknown load type: %s", cfg.Load.Type) // logam eroarea
		return fmt.Errorf("unknown load type: %s", cfg.Load.Type)   // returnam eroarea
	}
}
