package connector

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ExtractJSON(cfg map[string]interface{}) ([]string, [][]string, error) { // functie pentru extragerea datelor dintr-un fisier JSON
	path := cfg["path"].(string)                                   // obtinem calea fisierului din configuratie
	logger.Info.Printf("Starting JSON extraction | file=%s", path) // logam calea fisierului

	data, err := os.ReadFile(path) // citim continutul fisierului
	if err != nil {                // daca apare o eroare la citire
		logger.Error.Printf("Failed to read JSON file %s: %v", path, err) // logam eroarea
		return nil, nil, fmt.Errorf("cannot read JSON: %w", err)          // returnam eroarea
	}

	var items []map[string]interface{}                   // slice pentru a tine obiectele JSON
	if err := json.Unmarshal(data, &items); err != nil { // deserializam JSON-ul
		logger.Error.Printf("Invalid JSON structure: %v", err) // logam eroarea
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)   // returnam eroarea
	}

	if len(items) == 0 { // daca lista este goala
		logger.Error.Printf("JSON file is empty: %s", path) // logam un mesaj de eroare
		return nil, nil, fmt.Errorf("JSON empty")           // returnam eroarea
	}

	header := make([]string, 0, len(items[0])) // header-ul va fi cheile primului obiect
	for key := range items[0] {                // iteram prin chei
		header = append(header, key) // adaugam cheia la header
	}

	var rows [][]string         // slice pentru a tine randurile
	for _, obj := range items { // iteram prin obiecte
		row := make([]string, 0, len(header)) // cream un nou rand
		for _, h := range header {            // iteram prin header
			row = append(row, fmt.Sprintf("%v", obj[h])) // adaugam valoarea corespunzatoare la rand
		}
		rows = append(rows, row) // adaugam randul la lista de randuri
	}

	logger.Info.Printf("JSON extracted successfully | rows=%d cols=%d", len(rows), len(header)) // logam numarul de randuri si coloane extrase
	return header, rows, nil                                                                    // returnam header-ul, randurile si nil pentru eroare
}
