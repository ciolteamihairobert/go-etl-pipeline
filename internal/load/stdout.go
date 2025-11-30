package load

import (
	"fmt"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ToStdout(header []string, rows [][]string) error { // functie pentru incarcarea datelor in stdout
	logger.Info.Println("Starting load -> stdout") // logam un mesaj de start al incarcarii

	fmt.Println("OUTPUT HEADER:") // afisam header-ul
	fmt.Println(header)           // afisam header-ul

	fmt.Println("OUTPUT ROWS:") // afisam randurile
	for _, r := range rows {    // iteram prin randuri
		fmt.Println(r) // afisam randul curent
	}

	logger.Info.Printf("Load to stdout finished | %d rows output", len(rows)) // logam un mesaj de finalizare a incarcarii

	return nil // returnam nil la final
}
