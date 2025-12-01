package load

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
	_ "github.com/lib/pq"
)

func ToPostgres(cfg map[string]interface{}, header []string, rows [][]string) error { // functie pentru incarcarea datelor in PostgreSQL
	logger.Info.Println("POSTGRES LOAD: Starting PostgreSQL load operation...") // logam un mesaj de start

	connStr, ok := cfg["conn"].(string) // stringul de conexiune la baza de date
	if !ok {                            // daca nu exista cheia "conn" sau nu este string
		logger.Error.Println("POSTGRES LOAD ERROR: missing 'conn' in config") // logam eroarea
		return fmt.Errorf("postgres load missing 'conn'")                     // returnam eroarea
	}

	table, ok := cfg["table"].(string) // numele tabelului in care se vor incarca datele
	if !ok {                           // daca nu exista cheia "table" sau nu este string
		logger.Error.Println("POSTGRES LOAD ERROR: missing 'table' in config") // logam eroarea
		return fmt.Errorf("postgres load missing 'table'")                     // returnam eroarea
	}

	logger.Info.Printf("POSTGRES LOAD: Connecting using conn=%s", connStr) // logam stringul de conexiune

	db, err := sql.Open("postgres", connStr) // deschidem conexiunea la baza de date
	if err != nil {                          // daca apare o eroare la deschidere
		logger.Error.Printf("POSTGRES LOAD: Failed opening connection: %v", err) // logam eroarea
		return err                                                               // returnam eroarea
	}
	defer db.Close() // inchidem conexiunea la final

	logger.Info.Printf("POSTGRES LOAD: Preparing CREATE TABLE for %s", table) // logam un mesaj de creare a tabelului

	cols := make([]string, len(header)) // slice pentru definitiile coloanelor
	for i, h := range header {          // iteram prin antet
		cols[i] = fmt.Sprintf(`"%s" TEXT`, h) // definim coloana ca TEXT
	}

	createQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s);`,
		table,
		strings.Join(cols, ", "),
	) // construim query-ul de creare a tabelului

	logger.Info.Println("POSTGRES LOAD: Executing:", createQuery) // logam query-ul de creare a tabelului

	if _, err := db.Exec(createQuery); err != nil { // executam query-ul de creare a tabelului
		logger.Error.Printf("POSTGRES LOAD: Failed CREATE TABLE: %v", err) // logam eroarea
		return fmt.Errorf("failed to create table: %w", err)               // returnam eroarea
	}

	logger.Info.Printf("POSTGRES LOAD: Preparing INSERT into %s", table) // logam un mesaj de pregatire a insert-ului

	placeholders := make([]string, len(header)) // slice pentru placeholder-ele din query
	for i := range placeholders {               // iteram prin placeholder-e
		placeholders[i] = fmt.Sprintf("$%d", i+1) // definim placeholder-ul ca $1, $2, ...
	}

	insertQuery := fmt.Sprintf(
		`INSERT INTO "%s" ("%s") VALUES (%s);`,
		table,
		strings.Join(header, `","`),
		strings.Join(placeholders, ","),
	) // construim query-ul de insert

	logger.Info.Println("POSTGRES LOAD: Insert query:", insertQuery) // logam query-ul de insert

	stmt, err := db.Prepare(insertQuery) // pregatim statement-ul de insert
	if err != nil {                      // daca apare o eroare la pregatire
		logger.Error.Printf("POSTGRES LOAD: Failed preparing insert: %v", err) // logam eroarea
		return fmt.Errorf("failed preparing insert: %w", err)                  // returnam eroarea
	}
	defer stmt.Close() // inchidem statement-ul la final

	for idx, row := range rows { // iteram prin randurile de date
		vals := make([]interface{}, len(row)) // slice pentru valorile randului
		for i := range row {                  // iteram prin coloane
			vals[i] = row[i] // atribuim valoarea coloanei
		}

		if _, err := stmt.Exec(vals...); err != nil { // executam insert-ul pentru randul curent
			logger.Error.Printf("POSTGRES LOAD: Insert failed on row %d: %v | data=%v", idx, err, row) // logam eroarea
			return fmt.Errorf("insert failed: %w", err)                                                // returnam eroarea
		}
	}

	logger.Info.Printf("POSTGRES LOAD: Completed successfully. Inserted %d rows", len(rows)) // logam un mesaj de succes
	return nil                                                                               // returnam nil pentru eroare
}
