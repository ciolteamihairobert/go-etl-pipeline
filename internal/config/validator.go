package config

import (
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func (cfg *PipelineConfig) Validate() error { // functie pentru validarea configuratiei pipeline-ului
	logger.Info.Println("Starting config validation...") // logam un mesaj de start al validarii

	if cfg.Name == "" { // daca numele pipeline-ului este gol
		logger.Error.Println("Config validation failed: pipeline name missing") // logam eroarea
		return fmt.Errorf("pipeline name is required")                          // returnam eroarea
	}

	if cfg.Extract.Type == "" { // daca tipul de extractie este gol
		logger.Error.Println("Config validation failed: extract type missing") // logam eroarea
		return fmt.Errorf("extract type is required")                          // returnam eroarea
	}

	if cfg.Load.Type == "" { // daca tipul de incarcare este gol
		logger.Error.Println("Config validation failed: load type missing") // logam eroarea
		return fmt.Errorf("load type is required")                          // returnam eroarea
	}

	if cfg.Schedule.IntervalSeconds <= 0 || cfg.Schedule.Retries < 1 { // daca setarile de schedule sunt invalide
		logger.Error.Printf("Invalid schedule settings: interval=%d retries=%d", cfg.Schedule.IntervalSeconds, cfg.Schedule.Retries) // logam eroarea
		return fmt.Errorf("schedule interval must be > 0 and retries must be >= 1")                                                  // returnam eroarea
	}

	for i, t := range cfg.Transform { // iteram prin pasii de transformare
		if t.Type == "" { // daca tipul pasului este gol
			logger.Error.Printf("Transform step %d has no type", i+1) // logam eroarea
			return fmt.Errorf("transform step %d has no type", i+1)   // returnam eroarea
		}
		if t.Type == "aggregate" { // daca tipul este aggregate
			if t.GroupBy == "" { // daca nu este specificat group_by
				logger.Error.Printf("Aggregate step %d missing group_by", i+1) // logam eroarea
				return fmt.Errorf("aggregate step %d missing 'group_by'", i+1) // returnam eroarea
			}
			if len(t.Operations) == 0 { // daca nu sunt specificate operatiile
				logger.Error.Printf("Aggregate step %d missing operations", i+1) // logam eroarea
				return fmt.Errorf("aggregate step %d has no operations", i+1)    // returnam eroarea
			}
		}
	}

	for i, r := range cfg.DataValidation { // iteram prin regulile de validare a datelor
		if r.Field == "" || r.Rule == "" { // daca field sau rule sunt goale
			logger.Error.Printf("Validation rule %d incomplete: field='%s', rule='%s'", i+1, r.Field, r.Rule) // logam eroarea
			return fmt.Errorf("validation rule %d must have both 'field' and 'rule'", i+1)                    // returnam eroarea
		}
	}

	logger.Info.Println("Config validation passed successfully.") // logam un mesaj de succes
	return nil                                                    // returnam nil daca totul este valid
}
