package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// an example programatic client that every 3 seconds:
//
//	calls tx endpoint of a server
//	truncates old data
//	loads new data
func main() {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	for {

		// HTTP request
		endpoint := "http://localhost:2001/tx?table=event"
		log.Println("getting", endpoint)
		resp, err := http.Get(endpoint)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			txt, _ := io.ReadAll(resp.Body)
			err = fmt.Errorf("host responded %d: %s", resp.StatusCode, txt)
			log.Fatal(err)
			return
		}

		targetTable := "asdf"

		// truncate old data
		_, err = conn.Exec(ctx, "truncate "+targetTable)
		if err != nil {
			log.Fatal(err)
			return
		}

		// copy new data
		copySql := fmt.Sprintf("COPY %s FROM STDIN", targetTable)
		cmd, err := conn.PgConn().CopyFrom(ctx, resp.Body, copySql)
		if err != nil {
			log.Fatal(err)
			return
		}

		log.Println("OK rows =", cmd.RowsAffected())
		time.Sleep(time.Second * 3)
	}

}
