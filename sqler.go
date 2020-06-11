// Package sqler wraps database/sql in interfaces.
package sqler

import (
	"context"
	"database/sql"
	"time"
)

// Open is a convenience function for calling sql.Open and wrapping the result
// with WrapDB.
func Open(driverName, dataSourceName string) (DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return WrapDB(db), nil
}

type Queryer interface {
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(ctx context.Context, query string) (Stmt, error)
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) Row
}

type DB interface {
	Conn
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
	Unwrap() *sql.DB
}

type Conn interface {
	Queryer
	Close() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Ping(ctx context.Context) error
}

type Tx interface {
	Queryer
	Commit() error
	Rollback() error
	Stmt(ctx context.Context, stmt Stmt) Stmt
}

type Stmt interface {
	Close() error
	Exec(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(ctx context.Context, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, args ...interface{}) Row
	Unwrap() *sql.Stmt
}

type Rows interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...interface{}) error
}

type Row interface {
	Scan(dest ...interface{}) error
}

// SQLQueryer is the common interface of *sql.DB, *sql.Conn and *sql.Tx.
type SQLQueryer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// SQLConn is the common interface of *sql.DB and *sql.Conn.
type SQLConn interface {
	SQLQueryer
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	PingContext(ctx context.Context) error
}

func WrapDB(db *sql.DB) DB {
	return wrappedDB{db, WrapQueryer(db)}
}

func WrapConn(c SQLConn) Conn {
	return wrappedConn{c, WrapQueryer(c)}
}

func WrapQueryer(q SQLQueryer) Queryer {
	return wrappedQueryer{q}
}

func WrapStmt(stmt *sql.Stmt) Stmt {
	return wrappedStmt{stmt}
}

type wrappedDB struct {
	*sql.DB
	queryer Queryer
}

func (db wrappedDB) Conn(ctx context.Context) (Conn, error) {
	c, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return WrapConn(c), nil
}

func (db wrappedDB) Unwrap() *sql.DB {
	return db.DB
}

func (db wrappedDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return wrappedTx{tx, WrapQueryer(tx)}, nil
}

type connTx struct {
	c Conn
	Tx
}

func (tx connTx) Commit() error {
	defer tx.c.Close()
	return tx.Tx.Commit()
}

func (tx connTx) Rollback() error {
	defer tx.c.Close()
	return tx.Tx.Rollback()
}

func (db wrappedDB) Ping(ctx context.Context) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Ping(ctx)
}

func (db wrappedDB) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.queryer.Exec(ctx, query, args...)
}

func (db wrappedDB) Prepare(ctx context.Context, query string) (Stmt, error) {
	return db.queryer.Prepare(ctx, query)
}

func (db wrappedDB) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return db.queryer.Query(ctx, query, args...)
}

func (db wrappedDB) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return db.queryer.QueryRow(ctx, query, args...)
}

type errRow struct {
	err error
}

func (err errRow) Scan(dest ...interface{}) error { return err.err }

type wrappedConn struct {
	c SQLConn
	Queryer
}

func (c wrappedConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := c.c.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return wrappedTx{tx, WrapQueryer(tx)}, nil
}

func (c wrappedConn) Close() error {
	return c.c.Close()
}

func (c wrappedConn) Ping(ctx context.Context) error {
	return c.c.PingContext(ctx)
}

type wrappedTx struct {
	tx *sql.Tx
	Queryer
}

func (tx wrappedTx) Commit() error {
	return tx.tx.Commit()
}
func (tx wrappedTx) Rollback() error {
	return tx.tx.Rollback()
}
func (tx wrappedTx) Stmt(ctx context.Context, stmt Stmt) Stmt {
	return WrapStmt(tx.tx.StmtContext(ctx, stmt.Unwrap()))
}

type wrappedQueryer struct {
	SQLQueryer
}

func (q wrappedQueryer) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	r, err := q.SQLQueryer.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (q wrappedQueryer) Prepare(ctx context.Context, query string) (Stmt, error) {
	s, err := q.SQLQueryer.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return WrapStmt(s), nil
}

func (q wrappedQueryer) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := q.SQLQueryer.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (q wrappedQueryer) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return q.SQLQueryer.QueryRowContext(ctx, query, args...)
}

type wrappedStmt struct {
	*sql.Stmt
}

func (s wrappedStmt) Exec(ctx context.Context, args ...interface{}) (sql.Result, error) {
	r, err := s.Stmt.ExecContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s wrappedStmt) Query(ctx context.Context, args ...interface{}) (Rows, error) {
	w, err := s.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s wrappedStmt) QueryRow(ctx context.Context, args ...interface{}) Row {
	return s.Stmt.QueryRowContext(ctx, args...)
}

func (s wrappedStmt) Unwrap() *sql.Stmt {
	return s.Stmt
}
