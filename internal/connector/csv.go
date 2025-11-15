package connector

import (
	"encoding/csv"
	"fmt"
	"os"
)

func ExtractCSV(cfg map[string]interface{}) ([]string, [][]string, error) { // functie pentru extragerea datelor dintr-un fisier CSV
	path := cfg["path"].(string) // obtinem calea fisierului din configuratie
	file, err := os.Open(path)   // deschidem fisierul CSV
	if err != nil {              // daca apare o eroare la deschidere
		return nil, nil, err // returnam nil si eroarea
	}
	defer file.Close() // inchidem fisierul la finalul functiei

	reader := csv.NewReader(file) // cream un cititor CSV
	rows, err := reader.ReadAll() // citim toate randurile din fisier
	if err != nil {               // daca apare o eroare la citire
		return nil, nil, err // returnam eroarea
	}

	if len(rows) == 0 { // daca fisierul este gol
		return nil, nil, nil // returnam nil
	}

	header := rows[0]                                                   // primul rand este header-ul
	data := rows[1:]                                                    // restul sunt datele
	fmt.Printf("Extracted %d rows from CSV file %s\n", len(data), path) // afisam numarul de randuri extrase

	return header, data, nil // returnam header-ul, datele si nil pentru eroare
}
