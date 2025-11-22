package config

import (
	"fmt"
)

func (cfg *PipelineConfig) Validate() error { // functie pentru validarea configuratiei pipeline-ului
	if cfg.Name == "" { // daca numele pipeline-ului este gol
		return fmt.Errorf("pipeline name is required") // returnam o eroare
	}
	if cfg.Extract.Type == "" { // daca tipul de extractie este gol
		return fmt.Errorf("extract type is required") // returnam o eroare
	}
	if cfg.Load.Type == "" { // daca tipul de incarcare este gol
		return fmt.Errorf("load type is required") // returnam o eroare
	}

	// Validam ca intervalul e pozitiv si ca numarul de retry-uri e cel putin 1.
	if cfg.Schedule.IntervalSeconds <= 0 || cfg.Schedule.Retries < 1 { // daca intervalul <= 0 sau retries < 1
		return fmt.Errorf("schedule interval must be > 0 and retries must be >= 1") // returnam o eroare
	}

	for i, t := range cfg.Transform { // iteram prin pasii de transformare
		if t.Type == "" { // daca tipul pasului este gol
			return fmt.Errorf("transform step %d has no type", i+1) // returnam o eroare
		}
		if t.Type == "aggregate" { // daca tipul este agregare
			if t.GroupBy == "" { // daca nu este specificat campul de grupare
				return fmt.Errorf("aggregate step %d missing 'group_by'", i+1) // returnam o eroare
			}
			if len(t.Operations) == 0 { // daca nu sunt specificate operatiile
				return fmt.Errorf("aggregate step %d has no operations", i+1) // returnam o eroare
			}
		}
	}

	return nil
}
