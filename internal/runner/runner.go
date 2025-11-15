package runner

import (
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/connector"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/load"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/transform"
)

func Run(cfg *config.PipelineConfig) error { // functie pentru rularea pipeline-ului ETL
	header, rows, err := connector.ExtractCSV(cfg.Extract.Config) // extragem datele folosind configuratia de extractie
	if err != nil {                                               // daca apare o eroare la extragere
		return err // returnam eroarea
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
	}

	return nil // returnam nil daca totul a decurs bine
}
