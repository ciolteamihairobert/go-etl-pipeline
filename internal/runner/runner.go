package runner

import (
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/connector"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/load"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/transform"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/validation"
)

func Run(cfg *config.PipelineConfig) error { // functie pentru rularea pipeline-ului ETL
	if err := cfg.Validate(); err != nil { // validam configuratia pipeline-ului
		return fmt.Errorf("pipeline validation failed: %w", err) // returnam eroarea daca validarea esueaza
	}

	header, rows, err := connector.ExtractCSV(cfg.Extract.Config) // extragem datele folosind configuratia de extractie
	if err != nil {
		return fmt.Errorf("extract failed: %w", err)
	}

	if len(cfg.DataValidation) > 0 { // daca exista reguli de validare a datelor
		for i, r := range rows { // iteram prin randuri
			if err := validation.ValidateRow(header, r, cfg.DataValidation); err != nil { // validam randul curent
				return fmt.Errorf("data validation failed at row %d: %w", i+1, err) // returnam eroarea daca validarea esueaza
			}
		}
	}

	for _, step := range cfg.Transform { // iteram prin pasii de transformare
		switch step.Type { // tipul pasului de transformare
		case "filter": // daca este un filtru
			rows = transform.ApplyFilter(rows, header, step.Expression) // aplicam filtrul pe randuri

		case "map": // daca este o mapare
			header, rows = transform.ApplyMapping(rows, header, step.Mapping) // aplicam maparea pe randuri si header

		case "aggregate": // daca este o agregare
			header, rows = transform.Aggregate(rows, header, step.GroupBy, step.Operations) // aplicam agregarea pe randuri si header
		}
	}

	switch cfg.Load.Type { // tipul de incarcare
	case "stdout": // daca este stdout
		return load.ToStdout(header, rows) // incarcam datele in stdout
	case "sqlite": // daca este sqlite
		return load.ToSQLite(cfg.Load.Config, header, rows) // incarcam datele in sqlite
	default:
		return fmt.Errorf("unknown load type: %s", cfg.Load.Type)
	}
}
