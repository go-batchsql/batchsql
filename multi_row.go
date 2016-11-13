// Copyright (c) 2016, M Bogus.
// This source file is part of the batchsql open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

package batchsql

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const sqlComment = `(\/\*.*?\*\/)`

var (
	reSQLValues           = regexp.MustCompile(`.*VALUES(\n|\s)*?(?P<values>\(.+\)).*`)
	reEmptyValues         = regexp.MustCompile(`\(\s+\).*`)
	reSQLInsertStmt       = regexp.MustCompile(fmt.Sprintf(`(%s|\s)*INSERT(%s|\s)*INTO.+|\n*VALUES.*`, sqlComment, sqlComment))
	reMySQLOnDuplicateKey = regexp.MustCompile("\\s*ON\\s+DUPLICATE\\s+KEY(?:[^\"'`]*[\"'`][^\"'`]*[\"'`])*[^\"'`]*$")
)

// MultiRow is a wrapper around sql.DB or sql.Tx supporting
// multi-row insert statements execution with arbitrary
// batch size
type MultiRow struct {
	Conn      Preparable
	BatchSize int
}

// Preparable is an interface for any type able to prepare
// SQL statement
type Preparable interface {
	Prepare(query string) (*sql.Stmt, error)
}

// CheckQuery is checking if query string is eligible for rewriting into a multi-row insert.
func CheckQuery(query string) error {
	if reMySQLOnDuplicateKey.MatchString(query) {
		query = reMySQLOnDuplicateKey.ReplaceAllString(query, "")
	}

	if !reSQLInsertStmt.MatchString(query) {
		return errors.New("Invalid statement for multi-row INSERT: INSERT [ INTO ] <object> [ column_list ] VALUES sequence not recognized")
	}
	if !reSQLValues.MatchString(query) {
		return errors.New("Invalid statement for multi-row INSERT: ... VALUES (? [, ...?]) ... sequence not recognized")
	}
	return nil
}

// MultiInsert is executing multi-row insert query iterating over the list of
// arguments in args. The query string is checked for eligibility for multi-row
// rewrite. INSERT statements are optimized by batching the data using common
// multi-row syntax and results are discarded
func (conn *MultiRow) MultiInsert(
	query string,
	args ...([]interface{})) error {

	if len(args) == 0 {
		return errors.New("Invalid multi-row INSERT call with no arguments")
	}

	if err := CheckQuery(query); err != nil {
		return err
	}

	var err error
	var size int
	var stmt *sql.Stmt

	values, err := extractValues(query)

	if err != nil {
		return err
	}

	for i := 0; i < len(args); i += conn.BatchSize {

		j := min(i+conn.BatchSize, len(args))
		batch := args[i:j]

		if stmt == nil || len(batch) != size {
			if stmt != nil {
				stmt.Close()
			}
			size = len(batch)
			multiQuery := rewriteInsertSQL(&query, &values, size)
			stmt, err = conn.Conn.Prepare(multiQuery)
			if err != nil {
				return err
			}
		}

		batchArgs := joinArgs(batch)
		_, err := stmt.Exec(batchArgs...)
		if err != nil {
			stmt.Close()
			return err
		}
	}
	return stmt.Close()
}

func extractValues(query string) (string, error) {

	if reMySQLOnDuplicateKey.MatchString(query) {
		query = reMySQLOnDuplicateKey.ReplaceAllString(query, "")
	}
	names := reSQLValues.SubexpNames()
	matches := reSQLValues.FindAllStringSubmatch(query, -1)
	err := errors.New("Unable to parse VALUES (? [, ...?]) sequence")
	if matches == nil || len(matches) == 0 {
		return "", err
	}
	for i, name := range names {
		if name == "values" {
			if v := matches[0][i]; !reEmptyValues.Match([]byte(v)) {
				return v, nil
			}
			break
		}
	}
	return "", err
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func rewriteInsertSQL(query *string, params *string, count int) string {

	arr := make([]string, count, count)
	for i := range arr {
		arr[i] = *params
	}

	return strings.Replace(*query, *params, strings.Join(arr, ","), 1)
}

func joinArgs(args []([]interface{})) []interface{} {
	var joined []interface{}
	for _, p := range args {
		joined = append(joined, p...)
	}
	return joined
}
