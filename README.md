# batchsql

Simple multi-row INSERT support for Go SQL

INSERT statements are optimized by batching the data, that is
using the multiple rows syntax.

Example: Inserting 3 new records to key, value columns

original statement:

~~~
INSERT INTO key_value (key, value) VALUES (?,?)
~~~

the statement is rewritten using the multi-row syntax and
executed as a prepared statement:

~~~
INSERT INTO key_value (key, value) VALUES (?,?),(?,?),(?,?)
~~~

Multi-row statements are batched to allow for better control of database
load. Some database engines put a limit on max number of multi-row values
and even if a limit is not specified, unnecessarily large batch size might
actually harm performance. Make sure to test performance of batched
queries using variety of batch sizes and use an optimal value that offers
best speed to database load ratio. Good starting points are 2000, 5000
and 10000.

The module exposes a utility function `CheckQuery`. Do yourself a favour and
run tests on queries through it to avoid any nasty surprises (not all
queries are eligible for multi-row rewrite and it's better to learn about
any issues prior to executing a query on an engine).

## Status

*Alpha*

[![Build Status](https://travis-ci.org/go-batchsql/batchsql.svg?branch=master)](https://travis-ci.org/go-batchsql/batchsql)  [![Coverage Status](https://coveralls.io/repos/github/go-batchsql/batchsql/badge.svg?branch=master)](https://coveralls.io/github/go-batchsql/batchsql?branch=master)

## Installation

### Install:

~~~go
go get gopkg.in/batchsql.v0
~~~


### Import:

~~~go
import "gopkg.in/batchsql.v0"
~~~

## Usage

Multi-row batch insert can be used with `sql.Tx` or `sql.DB`. Sample
transactional usage with `mysql` driver and insert batch size `5000`:

~~~go
db, err := sql.Open("mysql", connStr)
if err != nil {
	panic(err)
}
defer db.Close()

// sql.Open doesn't open a connection. Validate DSN data:
if err = db.Ping(); err != nil {
	panic(err)
}

query := "INSERT INTO t (c1,c2,c3) VALUES (?,?,CURRENT_TIMESTAMP)"

var args []([]interface{})
args = append(args, []interface{}{"value#1-1", "value#1-2"})
args = append(args, []interface{}{"value#2-1", "value#2-2"})
args = append(args, []interface{}{"value#3-1", "value#3-2"})
...
args = append(args, []interface{}{"value#100000-1", "value#100000-2"}

tx, err := db.Begin()
if err != nil {
	panic(err)
}
mr := &batchsql.MultiRow{Conn: tx, BatchSize: 5000}
if err = mr.MultiInsert(query, args...); err != nil {
	tx.Rollback()
	panic(err)
}
tx.Commit()
~~~


## License

Apache License Version 2.0 - See LICENSE file for more details
