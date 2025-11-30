package load

import (
	"database/sql"
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
	_ "modernc.org/sqlite"
)

func ToSQLite(cfg map[string]interface{}, header []string, rows [][]string) error {
	path := cfg["path"].(string)   // calea catre fisierul sqlite
	table := cfg["table"].(string) // numele tabelului unde se vor incarca datele

	logger.Info.Printf("Opening SQLite DB: %s", path) // logam calea bazei de date

	db, err := sql.Open("sqlite", path) // deschidem conexiunea la baza de date
	if err != nil {                     // daca apare o eroare la deschidere
		logger.Error.Printf("Failed to open DB: %v", err) // logam eroarea
		return err                                        // returnam eroarea
	}
	defer db.Close() // inchidem conexiunea la final

	logger.Info.Printf("Creating table: %s", table) // logam numele tabelului

	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s TEXT", table, header[0]) // cream query-ul de creare a tabelului
	for _, col := range header[1:] {                                                       // iteram prin coloanele header-ului
		createQuery += fmt.Sprintf(", %s TEXT", col) // adaugam coloana in query
	}
	createQuery += ")" // inchidem paranteza

	if _, err := db.Exec(createQuery); err != nil { // executam query-ul de creare a tabelului
		logger.Error.Printf("Create table failed: %v", err) // logam eroarea
		return err                                          // returnam eroarea
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, placeholders(len(header))) // cream query-ul de insert
	stmt, err := db.Prepare(insertQuery)                                                       // pregatim statement-ul de insert
	if err != nil {                                                                            // daca apare o eroare la pregatire
		logger.Error.Printf("Prepare insert failed: %v", err) // logam eroarea
		return err                                            // returnam eroarea
	}
	defer stmt.Close() // inchidem statement-ul la final

	for _, r := range rows { // iteram prin randuri
		vals := make([]interface{}, len(r)) // cream un slice de interfete pentru valorile randului
		for i := range r {                  // iteram prin valorile randului
			vals[i] = r[i] // atribuim valoarea la interfata corespunzatoare
		}

		if _, err := stmt.Exec(vals...); err != nil { // executam insert-ul cu valorile randului
			logger.Error.Printf("Insert failed: %v", err) // logam eroarea
			return err                                    // returnam eroarea
		}
	}

	logger.Info.Printf("Inserted %d rows into database.", len(rows)) // logam numarul de randuri inserate
	return nil                                                       // returnam nil pentru succes
}

func placeholders(n int) string { // functie pentru generarea placeholder-elor
	s := "?"                 // incepem cu primul placeholder
	for i := 1; i < n; i++ { // iteram de la 1 la n-1
		s += ",?" // adaugam un nou placeholder
	}
	return s // returnam sirul de placeholder-e
}
