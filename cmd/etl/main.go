package main

import (
	"fmt"
	"log"

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

	if err := runner.Run(cfg); err != nil { // rulam pipeline-ul
		log.Fatalf("Pipeline failed: %v", err) // logam eroarea si oprim executia daca apare o eroare
	}
}
