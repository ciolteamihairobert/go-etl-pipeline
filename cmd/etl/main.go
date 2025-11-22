package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/runner"
)

func main() {
	fmt.Println("Starting Go ETL Pipeline Builder") // mesaj de start

	cfg, err := config.LoadPipelineConfig("./examples/pipeline.yml") // incarcam configuratia pipeline-ului din fisierul yaml
	if err != nil {                                                  // daca apare o eroare la incarcare
		log.Fatalf("Failed to load config: %v", err) // logam eroarea si oprim executia
	}

	fmt.Println("Pipeline loaded successfully!")       // mesaj de succes
	fmt.Printf("Pipeline name: %s\n", cfg.Name)        // afisam numele pipeline-ului
	fmt.Printf("Extract type: %s\n", cfg.Extract.Type) // afisam tipul de extractie
	fmt.Printf("Load type: %s\n", cfg.Load.Type)       // afisam tipul de incarcare

	// scheduler simplu: rulam pipeline-ul periodic (la fiecare invervalSeconds secunde)
	// time.NewTicker creeaza un canal care trimite un eveniment la fiecare durata specificata
	ticker := time.NewTicker(time.Duration(cfg.Schedule.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		fmt.Println("Starting pipeline execution...")

		// retry pentru tot pipeline-ul
		err := retry(cfg.Schedule.Retries, func() error {
			return runner.Run(cfg)
		})

		if err != nil {
			log.Printf("Pipeline failed after retries: %v\n", err)
		} else {
			fmt.Println("Pipeline executed successfully!")
		}

		<-ticker.C // așteptăm următorul interval
	}
}

func retry(attempts int, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		wait := time.Second * time.Duration(i+1)
		fmt.Printf("Attempt %d failed: %v. Retrying in %v...\n", i+1, err, wait)
		time.Sleep(wait)
	}
	return fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}
