// Copyright (c) 2016, M Bogus.
// This source file is part of the batchsql open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

package batchsql

import (
	"database/sql"
	"errors"
	"testing"
)

func TestExtractValuesMySQLNoColumns(t *testing.T) {
	query := `INSERT INTO
    t1
VALUES
    (?, CURRENT_TIMESTAMP,?)`

	if err := CheckQuery(query); err != nil {
		t.Error(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Fatal(err)
	}

	if got, want := values, "(?, CURRENT_TIMESTAMP,?)"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesMySQLMultiline(t *testing.T) {
	query := `INSERT INTO
	t1
        ("c", "t", "d")
VALUES
    (?, CURRENT_TIMESTAMP,?)`

	if err := CheckQuery(query); err != nil {
		t.Error(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Fatal(err)
	}

	if got, want := values, "(?, CURRENT_TIMESTAMP,?)"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesMySQLOnDuplicateKey(t *testing.T) {
	query := "INSERT INTO `t1` (`c`, `t`, `d`) VALUES (?, CURRENT_TIMESTAMP,?) ON DUPLICATE KEY UPDATE `c` = VALUES (`c`), `d` = VALUES(`d`)"

	if err := CheckQuery(query); err != nil {
		t.Fatal(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Fatal(err)
	}

	if got, want := values, "(?, CURRENT_TIMESTAMP,?)"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesSQLServerWithColumns(t *testing.T) {
	query := `INSERT INTO Production.UnitMeasure (Name, UnitMeasureCode,
    ModifiedDate)
VALUES (N'Square Yards', N'Y2', GETDATE())`

	if err := CheckQuery(query); err != nil {
		t.Error(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Fatal(err)
	}

	if got, want := values, "(N'Square Yards', N'Y2', GETDATE())"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesSQLServerNoInto(t *testing.T) {
	query := `INSERT T1 (column_2) VALUES ('Row #2')`

	if err := CheckQuery(query); err != nil {
		t.Fatal(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Fatal(err)
	}

	if got, want := values, "('Row #2')"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesSELECTStatement(t *testing.T) {
	query := `SELECT 1 FROM dual`

	_, err := extractValues(query)

	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Unable to parse VALUES (? [, ...?]) sequence"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestExtractValuesBrokenInsert(t *testing.T) {
	query := `INSERT INTO t (x,y,z) VALUES ( )`

	_, err := extractValues(query)

	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Unable to parse VALUES (? [, ...?]) sequence"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestCheckQuerySelectInto(t *testing.T) {
	query := `SELECT [a], [b], [c]
INTO
    t2
FROM t1
`

	err := CheckQuery(query)
	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Invalid statement for multi-row INSERT: INSERT [ INTO ] <object> [ column_list ] VALUES sequence not recognized"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestCheckQueryInsertIntoSelect(t *testing.T) {
	query := `INSERT INTO
    t2 (a,b,c)
SELECT [a], [b], [c] FROM t1
`

	err := CheckQuery(query)
	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Invalid statement for multi-row INSERT: INSERT [ INTO ] <object> [ column_list ] VALUES sequence not recognized"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestCheckQuerySQLServerInsertDefaultValues(t *testing.T) {
	query := `INSERT INTO T1 DEFAULT VALUES`

	err := CheckQuery(query)
	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Invalid statement for multi-row INSERT: ... VALUES (? [, ...?]) ... sequence not recognized"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestCheckQueryMySQLSelectIntoOnDuplicateKey(t *testing.T) {
	query := `INSERT INTO tbl_temp2 (fld_id)
  SELECT tbl_temp1.fld_order_id
  FROM tbl_temp1 WHERE tbl_temp1.fld_order_id > 100
  ON DUPLICATE KEY UPDATE fld_id=VALUES(fld_id)+VALUES(fld_id)
`

	err := CheckQuery(query)
	if err == nil {
		t.Fatal(errors.New("Error expected"))
	}

	if got, want := err.Error(), "Invalid statement for multi-row INSERT: ... VALUES (? [, ...?]) ... sequence not recognized"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestRewriteInsertSQLOneValueTuple(t *testing.T) {
	query := `
    INSERT INTO
        tbl
    VALUES
        (?,?,?)
    `

	if err := CheckQuery(query); err != nil {
		t.Error(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Error(err)
	}

	multiQuery := rewriteInsertSQL(&query, &values, 1)

	if got, want := multiQuery, query; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestRewriteInsertSQLThreeValueTuples(t *testing.T) {
	query := `
    INSERT
        tbl
    VALUES
        (?,?,?)
    `
	if err := CheckQuery(query); err != nil {
		t.Error(err)
	}

	values, err := extractValues(query)

	if err != nil {
		t.Error(err)
	}

	multiQuery := rewriteInsertSQL(&query, &values, 3)

	if got, want := multiQuery, `
    INSERT
        tbl
    VALUES
        (?,?,?),(?,?,?),(?,?,?)
    `; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}
}

func TestJoinArgsEmpty(t *testing.T) {
	var args []([]interface{})
	if got := joinArgs(args); len(got) > 0 {
		t.Errorf("Expected empty got '%d' elements", len(got))
	}
}

func TestJoinArgs(t *testing.T) {
	var args []([]interface{})
	args = append(args, []interface{}{1, 2, 3})
	args = append(args, []interface{}{4, 5, 6})
	joinedArgs := joinArgs(args)
	if len(joinedArgs) == 0 {
		t.Errorf("Expected 6 elements, got '%d'", len(joinedArgs))
	}
	for i, x := range joinedArgs {
		if got, want := x, i+1; got != want {
			t.Errorf("Expected: '%d', got '%d'", want, got)
		}
	}
}

func TestMinEqual(t *testing.T) {
	if got, want := min(1, 1), 1; got != want {
		t.Errorf("Expected: '%d', got '%d'", want, got)
	}
}

func TestMinFirstGreater(t *testing.T) {
	if got, want := min(3, 2), 2; got != want {
		t.Errorf("Expected: '%d', got '%d'", want, got)
	}
}

func TestMinSecondGreater(t *testing.T) {
	if got, want := min(4, 5), 4; got != want {
		t.Errorf("Expected: '%d', got '%d'", want, got)
	}
}

type errPrepareDB struct {
}

func (db errPrepareDB) Prepare(query string) (*sql.Stmt, error) {
	return nil, errors.New("Unable to prepare any statement")
}

func TestMultiInsertFailedPrepare(t *testing.T) {
	db := errPrepareDB{}

	mr := MultiRow{Conn: db, BatchSize: 5}

	qry := "INSERT INTO t VALUES (?)"

	count := 12
	var args []([]interface{})
	for i := 0; i < count; i++ {
		args = append(args, []interface{}{i + 1})
	}

	err := mr.MultiInsert(qry, args...)
	if err == nil {
		t.Error(errors.New("Error expected"))
		return
	}

	if got, want := err.Error(), "Unable to prepare any statement"; got != want {
		t.Errorf("Expected: '%s', got '%s'", want, got)
	}

}
