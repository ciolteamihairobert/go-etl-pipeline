package main

import (
	"fmt"
	"log"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/connector"
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

	header, rows, err := connector.ExtractCSV(cfg.Extract.Config) // extragem datele folosind configuratia de extractie
	if err != nil {                                               // daca apare o eroare la extragere
		log.Fatalf("CSV extract failed: %v", err) // logam eroarea si oprim executia
	}

	fmt.Println("Header:", header) // afisam header-ul
	fmt.Println("Rows:", rows)     // afisam randurile
}
