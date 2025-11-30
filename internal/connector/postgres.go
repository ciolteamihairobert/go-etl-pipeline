package connector

import (
	"database/sql"
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
	_ "github.com/lib/pq"
)

func ExtractPostgres(cfg map[string]interface{}) ([]string, [][]string, error) { // functie pentru extragerea datelor din PostgreSQL
	connStr := cfg["conn"].(string) // stringul de conexiune la baza de date
	query := cfg["query"].(string)  // query-ul SQL pentru extragerea datelor

	logger.Info.Printf("Connecting to PostgreSQL...") // logam un mesaj de conectare

	db, err := sql.Open("postgres", connStr) // deschidem conexiunea la baza de date
	if err != nil {                          // daca apare o eroare la deschidere
		logger.Error.Printf("Failed to open PostgreSQL connection: %v", err) // logam eroarea
		return nil, nil, fmt.Errorf("pg open failed: %w", err)               // returnam eroarea
	}
	defer db.Close() // inchidem conexiunea la final

	logger.Info.Printf("Executing PostgreSQL query: %s", query) // logam query-ul executat

	rows, err := db.Query(query) // executam query-ul
	if err != nil {              // daca apare o eroare la executie
		logger.Error.Printf("PostgreSQL query failed: %v", err) // logam eroarea
		return nil, nil, fmt.Errorf("pg query failed: %w", err) // returnam eroarea
	}
	defer rows.Close() // inchidem rezultatele la final

	header, err := rows.Columns() // obtinem numele coloanelor
	if err != nil {               // daca apare o eroare la obtinere
		logger.Error.Printf("Failed to read PostgreSQL column names: %v", err) // logam eroarea
		return nil, nil, err                                                   // returnam eroarea
	}

	var result [][]string // slice pentru a tine randurile extrase
	for rows.Next() {     // iteram prin randurile rezultate
		values := make([]interface{}, len(header)) // cream un slice de interfete pentru valorile coloanelor
		ptrs := make([]interface{}, len(header))   // cream un slice de pointeri la interfete

		for i := range values { // iteram prin valori
			ptrs[i] = &values[i] // atribuim pointerul la interfata corespunzatoare
		}

		if err := rows.Scan(ptrs...); err != nil { // scanam valorile randului curent
			logger.Error.Printf("Failed scanning row: %v", err) // logam eroarea
			return nil, nil, err                                // returnam eroarea
		}

		row := make([]string, len(header)) // cream un slice pentru valorile randului ca stringuri
		for i, v := range values {         // iteram prin valori
			switch val := v.(type) { // facem type assertion pentru fiecare valoare
			case []byte: // daca valoarea este un byte slice
				row[i] = string(val) // convertim byte slice la string
			default:
				row[i] = fmt.Sprintf("%v", val) // convertim la string folosind Sprintf
			}
		}
		result = append(result, row) // adaugam randul la rezultat
	}

	logger.Info.Printf("PostgreSQL extracted %d rows, %d columns", len(result), len(header)) // logam numarul de randuri si coloane extrase
	return header, result, nil                                                               // returnam header-ul, randurile si nil pentru eroare
}
