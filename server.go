package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgBeamServer struct {
	pool *pgxpool.Pool
}

func NewPgBeamServer(pool *pgxpool.Pool) *PgBeamServer {
	return &PgBeamServer{
		pool,
	}
}

func (s *PgBeamServer) serveError(w http.ResponseWriter, r *http.Request, status int, err error) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}

func (s *PgBeamServer) serveCopyTo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// params
	qs := r.URL.Query()
	tableName := qs.Get("table")
	if tableName == "" {
		s.serveError(w, r, 400, errors.New("invalid table name"))
		return
	}
	// todo: check table name is allowed
	tableName = QuoteIdentifier(tableName)

	columns := "*"
	if columnList := qs.Get("select"); columnList != "" {
		columns = quoteColumnList(columnList)
	}

	where := ""
	whereClauses := []string{}
	for key, vals := range qs {
		if !strings.HasPrefix(key, "where.") {
			continue
		}
		phrase := strings.Split(key, ".")
		if len(phrase) != 3 {
			continue
		}
		column := QuoteIdentifier(phrase[1])

		addClause := func(op, predicate string) {
			clause := fmt.Sprintf("%s %s %s", column, op, predicate)
			whereClauses = append(whereClauses, clause)
		}

		switch phrase[2] {
		case "eq":
			addClause("=", QuoteString(vals[0]))
		case "gt":
			addClause(">", QuoteString(vals[0]))
		case "gte":
			addClause(">=", QuoteString(vals[0]))
		case "lt":
			addClause("<", QuoteString(vals[0]))
		case "lte":
			addClause("<=", QuoteString(vals[0]))
		case "in":
			addClause("IN", "("+quoteValueList(vals[0])+")")
		}
	}
	if len(whereClauses) > 0 {
		where = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// default is to copy everything
	query := tableName

	// if request has columns or wheres, custom query
	if columns != "*" || where != "" {
		query = fmt.Sprintf("(SELECT %s FROM %s %s)",
			columns,
			tableName,
			where,
		)
	}

	withOptions := ""
	if qs.Get("csv") != "" {
		// w.Header().Set("Content-Type", "text/csv")
		withOptions = "WITH (FORMAT csv, HEADER true)"
	}
	copySql := fmt.Sprintf("COPY %s TO STDOUT %s", query, withOptions)

	log.Println(copySql)

	// pg COPY TO
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		s.serveError(w, r, 500, err)
		return
	}
	defer conn.Release()

	_, err = conn.Conn().PgConn().CopyTo(ctx, w, copySql)
	if err != nil {
		s.serveError(w, r, 500, err)
		return
	}
}

func (s *PgBeamServer) serveCopyFrom(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	qs := r.URL.Query()

	host, err := url.Parse(qs.Get("host"))
	if err != nil {
		s.serveError(w, r, 400, err)
		return
	}

	to := qs.Get("to")
	if to == "" {
		s.serveError(w, r, 400, errors.New("to is required"))
		return
	}
	to = QuoteIdentifier(to)

	columns := ""
	if columnList := qs.Get("select"); columnList != "" {
		columns = "(" + quoteColumnList(columnList) + ")"
	}

	// HTTP request
	host.RawQuery = qs.Encode()
	log.Println("getting", host.String())
	resp, err := http.Get(host.String())
	if err != nil {
		s.serveError(w, r, 502, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		txt, _ := io.ReadAll(resp.Body)
		s.serveError(w, r, 502, fmt.Errorf("host responded %d: %s", resp.StatusCode, txt))
		return
	}

	// pgx COPY FROM
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		s.serveError(w, r, 500, err)
		return
	}
	defer conn.Release()

	withOptions := ""
	if qs.Get("csv") != "" {
		withOptions = "WITH (FORMAT csv, HEADER true)"
	}
	copySql := fmt.Sprintf("COPY %s %s FROM STDIN %s", to, columns, withOptions)
	_, err = conn.Conn().PgConn().CopyFrom(ctx, resp.Body, copySql)
	if err != nil {
		s.serveError(w, r, 502, err)
		return
	}

	w.Write([]byte("OK"))
}

func main() {

	dbUrl := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(context.Background(), dbUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	s := &PgBeamServer{pool}
	http.HandleFunc("/tx", s.serveCopyTo)
	http.HandleFunc("/rx", s.serveCopyFrom)

	fmt.Println("server on :2001")
	http.ListenAndServe(":2001", nil)

}

// copied from pgx codebase:

// QuoteString escapes and quotes a string making it safe for interpolation
// into an SQL string.
func QuoteString(input string) (output string) {
	output = "'" + strings.Replace(input, "'", "''", -1) + "'"
	return
}

// QuoteIdentifier escapes and quotes an identifier making it safe for
// interpolation into an SQL string
func QuoteIdentifier(input string) (output string) {
	output = `"` + strings.Replace(input, `"`, `""`, -1) + `"`
	return
}

// quote helpers

func quoteColumnList(commaList string) string {
	split := strings.Split(commaList, ",")
	quoted := make([]string, len(split))
	for idx, col := range split {
		quoted[idx] = QuoteIdentifier(col)
	}
	return strings.Join(quoted, ",")
}

func quoteValueList(valueList string) string {
	split := strings.Split(valueList, ",")
	quoted := make([]string, len(split))
	for idx, col := range split {
		quoted[idx] = QuoteString(col)
	}
	return strings.Join(quoted, ",")
}
