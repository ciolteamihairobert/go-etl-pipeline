package load

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

func ToSQLite(cfg map[string]interface{}, header []string, rows [][]string) error { // functie pentru incarcarea datelor in sqlite
	path, ok := cfg["path"].(string) // obtinem calea bazei de date din configuratie
	if !ok {                         // daca nu exista
		return fmt.Errorf("missing 'path' in config") // returnam o eroare
	}
	table, ok := cfg["table"].(string) // obtinem numele tabelului din configuratie
	if !ok {                           // daca nu exista
		return fmt.Errorf("missing 'table' in config") // returnam o eroare
	}

	db, err := sql.Open("sqlite", path) // deschidem conexiunea la baza de date
	if err != nil {                     // daca apare o eroare la deschidere
		return err // returnam eroarea
	}
	defer db.Close() // inchidem conexiunea la final

	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s TEXT", table, header[0]) // cream query-ul de creare a tabelului
	for _, col := range header[1:] {                                                       // iteram prin coloane
		createQuery += fmt.Sprintf(", %s TEXT", col) // adaugam fiecare coloana la query
	}
	createQuery += ")" // inchidem paranteza

	if _, err := db.Exec(createQuery); err != nil { // executam query-ul de creare a tabelului
		return fmt.Errorf("failed to create table: %v", err) // returnam eroarea daca apare
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, placeholders(len(header))) // cream query-ul de insert
	stmt, err := db.Prepare(insertQuery)                                                       // pregatim statement-ul de insert
	if err != nil {                                                                            // daca apare o eroare la pregatire
		return fmt.Errorf("failed to prepare insert: %v", err) // returnam eroarea
	}
	defer stmt.Close() // inchidem statement-ul la final

	for _, r := range rows { // iteram prin randuri
		vals := make([]interface{}, len(r)) // cream un slice pentru valorile de insert
		for i := range r {                  // iteram prin fiecare valoare
			vals[i] = r[i] // copiem valoarea in slice
		}
		if _, err := stmt.Exec(vals...); err != nil { // executam insert-ul
			return fmt.Errorf("failed to insert row %v: %v", r, err) // returnam eroarea daca apare
		}
	}

	fmt.Printf("Inserted %d rows into %s\n", len(rows), table) // afisam numarul de randuri inserate
	return nil
}

func placeholders(n int) string { // functie pentru generarea placeholder-elor
	s := "?"                 // incepem cu primul placeholder
	for i := 1; i < n; i++ { // iteram de la 1 la n-1
		s += ",?" // adaugam un nou placeholder
	}
	return s // returnam sirul de placeholder-e
}
