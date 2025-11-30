package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/runner"
)

func main() {
	// initialize logger (file + stdout)
	logger.Init()                                                   // apelam functia Init din pachetul logger pentru a initializa logger-ul
	logger.Info.Println("=== Starting Go ETL Pipeline Builder ===") // logam un mesaj de start

	cfg, err := config.LoadPipelineConfig("./examples/pipeline_postgres.yml") // incarcam configuratia pipeline-ului din fisierul YAML
	if err != nil {                                                           // daca apare o eroare la incarcare
		logger.Error.Printf("Failed to load config: %v", err) // logam eroarea
		log.Fatalf("Failed to load config: %v", err)          // logam eroarea si oprim executia
	}

	logger.Info.Printf("Pipeline loaded: %s | Extract: %s | Load: %s", cfg.Name, cfg.Extract.Type, cfg.Load.Type) // logam detalii despre pipeline

	ticker := time.NewTicker(time.Duration(cfg.Schedule.IntervalSeconds) * time.Second) // cream un ticker pentru scheduling
	defer ticker.Stop()                                                                 // oprim ticker-ul la final

	for {
		logger.Info.Println("---- Pipeline Execution Started ----") // logam un mesaj de start al executiei pipeline-ului

		err := retry(cfg.Schedule.Retries, func() error { // incercam sa rulam pipeline-ul cu retry-uri
			return runner.Run(cfg) // rulam pipeline-ul folosind configuratia incarcata
		})

		if err != nil { // daca apare o eroare dupa toate retry-urile
			logger.Error.Printf("Pipeline failed after retry attempts: %v", err) // logam eroarea
		} else {
			logger.Info.Println("Pipeline executed successfully.") // logam un mesaj de succes
		}

		logger.Info.Printf("Waiting %d seconds for next scheduled run...", cfg.Schedule.IntervalSeconds) // logam timpul de asteptare

		<-ticker.C // asteptam urmatorul tick pentru a rula din nou pipeline-ul
	}
}

func retry(attempts int, fn func() error) error { // functie pentru retry-uri
	var err error

	for i := 0; i < attempts; i++ { // iteram de la 0 la numarul de incercari
		err = fn()      // apelam functia transmisa ca parametru
		if err == nil { // daca nu apare nicio eroare
			return nil // returnam nil
		}

		wait := time.Second * time.Duration(i+1)                                                     // calculam timpul de asteptare inainte de urmatoarea incercare
		logger.Error.Printf("Attempt %d/%d failed: %v. Retrying in %v...", i+1, attempts, err, wait) // logam eroarea si timpul de asteptare

		time.Sleep(wait) // asteptam inainte de urmatoarea incercare
	}

	return fmt.Errorf("after %d attempts, last error: %w", attempts, err) // returnam eroarea dupa toate incercarile esuate
}
