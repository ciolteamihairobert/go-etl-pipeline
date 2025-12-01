package load

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ToCSV(cfg map[string]interface{}, header []string, rows [][]string) error { // functie pentru scrierea datelor intr-un fisier CSV
	path, ok := cfg["path"].(string) // calea fisierului CSV
	if !ok {                         // daca nu exista cheia "path" sau nu este string
		return fmt.Errorf("csv writer requires 'path'") // returnam eroarea
	}

	logger.Info.Printf("Writing CSV output to %s...", path) // logam un mesaj de scriere

	file, err := os.Create(path) // cream fisierul CSV
	if err != nil {              // daca apare o eroare la creare
		logger.Error.Printf("Failed creating file: %v", err) // logam eroarea
		return err                                           // returnam eroarea
	}
	defer file.Close() // inchidem fisierul la final

	w := csv.NewWriter(file) // cream un writer CSV
	defer w.Flush()          // asiguram scrierea datelor la final

	if err := w.Write(header); err != nil { // scriem antetul in fisier
		return err // returnam eroarea daca apare
	}

	for _, r := range rows { // iteram prin randurile de date
		if err := w.Write(r); err != nil { // scriem randul in fisier
			return err // returnam eroarea daca apare
		}
	}

	logger.Info.Printf("CSV written successfully with %d rows", len(rows)) // logam un mesaj de succes
	return nil
}
