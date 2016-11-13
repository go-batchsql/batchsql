// Copyright (c) 2016, M Bogus.
// This source file is part of the batchsql open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

// +build integration

package batchsql

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestMultiInsert(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE t (v INT)")
	if err != nil {
		t.Error(err)
		return
	}
	mr := MultiRow{Conn: db, BatchSize: 5}

	qry := "INSERT INTO t VALUES (?)"

	count := 12
	var args []([]interface{})
	for i := 0; i < count; i++ {
		args = append(args, []interface{}{i + 1})
	}

	err = mr.MultiInsert(qry, args...)
	if err != nil {
		t.Error(err)
		return
	}

	var dbCount int
	db.QueryRow("SELECT COUNT(1) FROM t").Scan(&dbCount)
	if count != dbCount {
		t.Errorf("Expected: %d got %d rows", count, dbCount)
	}
}

func TestMultiInsertNoArgs(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	mr := MultiRow{Conn: db, BatchSize: 5}

	var args []([]interface{})

	err = mr.MultiInsert("XXX", args...)
	if err == nil {
		t.Errorf("Expected error")
		return
	}

	if err.Error() != "Invalid multi-row INSERT call with no arguments" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestMultiInsertNoInsert(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	mr := MultiRow{Conn: db, BatchSize: 5}

	var args []([]interface{})
	args = append(args, []interface{}{1})
	err = mr.MultiInsert("UPDATE t SET v = 1", args...)

	if err == nil {
		t.Errorf("Expected error")
		return
	}

	if err.Error() != "Invalid statement for multi-row INSERT: INSERT [ INTO ] <object> [ column_list ] VALUES sequence not recognized" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestMultiInsertInvalidMultiInsert(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	mr := MultiRow{Conn: db, BatchSize: 5}

	var args []([]interface{})
	args = append(args, []interface{}{11})
	err = mr.MultiInsert("INSERT INTO t (v) SELECT a FROM b", args...)

	if err == nil {
		t.Errorf("Expected error")
		return
	}

	if err.Error() != "Invalid statement for multi-row INSERT: ... VALUES (? [, ...?]) ... sequence not recognized" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestMultiInsertInvalidMultiInsertNoValues(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	mr := MultiRow{Conn: db, BatchSize: 5}

	var args []([]interface{})
	args = append(args, []interface{}{11})
	err = mr.MultiInsert("INSERT INTO t (v) VALUES ( )", args...)

	if err == nil {
		t.Errorf("Expected error")
		return
	}

	if err.Error() != "Unable to parse VALUES (? [, ...?]) sequence" {
		t.Errorf("Unexpected error: %v", err)
	}
}
