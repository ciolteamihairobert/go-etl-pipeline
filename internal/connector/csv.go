package connector

import (
	"encoding/csv"
	"os"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ExtractCSV(cfg map[string]interface{}) ([]string, [][]string, error) { // functie pentru extragerea datelor dintr-un fisier CSV
	path := cfg["path"].(string)                     // obtinem calea fisierului din configuratie
	logger.Info.Printf("Opening CSV file: %s", path) // logam calea fisierului

	file, err := os.Open(path) // deschidem fisierul CSV
	if err != nil {            // daca apare o eroare la deschidere
		logger.Error.Printf("Failed to open CSV: %v", err) // logam eroarea
		return nil, nil, err                               // returnam nil si eroarea
	}
	defer file.Close() // inchidem fisierul la finalul functiei

	reader := csv.NewReader(file) // cream un cititor CSV
	rows, err := reader.ReadAll() // citim toate randurile din fisier
	if err != nil {               // daca apare o eroare la citire
		logger.Error.Printf("Failed reading CSV: %v", err) // logam eroarea
		return nil, nil, err                               // returnam eroarea
	}

	if len(rows) == 0 { // daca fisierul este gol
		logger.Error.Println("CSV file is empty!") // logam un mesaj de eroare
		return nil, nil, nil                       // returnam nil
	}

	header := rows[0]                                                             // primul rand este header-ul
	data := rows[1:]                                                              // restul sunt datele
	logger.Info.Printf("CSV loaded: %d rows, %d columns", len(data), len(header)) // logam numarul de randuri si coloane

	return header, data, nil // returnam header-ul, datele si nil pentru eroare
}
