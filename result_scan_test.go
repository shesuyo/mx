package mx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
)

const rowsMapTestDriverName = "mx_rowsmap_stub_test"

func init() {
	sql.Register(rowsMapTestDriverName, rowsMapStubDriver{})
}

type rowsMapStubDriver struct{}

func (rowsMapStubDriver) Open(name string) (driver.Conn, error) {
	return rowsMapStubConn{}, nil
}

type rowsMapStubConn struct{}

func (rowsMapStubConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (rowsMapStubConn) Close() error { return nil }

func (rowsMapStubConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (rowsMapStubConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == "SELECT empty FROM stub" {
		return &rowsMapEmptyStubRows{}, nil
	}
	if query == "SELECT single FROM stub" {
		return &rowsMapSingleStubRows{}, nil
	}
	if query == "SELECT scan_error FROM stub" {
		return &rowsMapScanErrStubRows{}, nil
	}
	if query == "SELECT tracked_scan_error FROM stub" {
		return &rowsMapTrackedCloseRows{}, nil
	}
	return &rowsMapStubRows{}, nil
}

type rowsMapStubRows struct {
	idx int
}

func (rowsMapStubRows) Columns() []string {
	return []string{"id", "name"}
}

func (rowsMapStubRows) Close() error { return nil }

func (r *rowsMapStubRows) Next(dest []driver.Value) error {
	rows := [][]driver.Value{
		{[]byte("7"), []byte("alice")},
		{nil, []byte("bob")},
	}
	if r.idx >= len(rows) {
		return io.EOF
	}
	copy(dest, rows[r.idx])
	r.idx++
	return nil
}

type rowsMapSingleStubRows struct {
	done bool
}

func (rowsMapSingleStubRows) Columns() []string {
	return []string{"single"}
}

func (rowsMapSingleStubRows) Close() error { return nil }

func (r *rowsMapSingleStubRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = []byte("7")
	r.done = true
	return nil
}

type rowsMapEmptyStubRows struct{}

func (rowsMapEmptyStubRows) Columns() []string {
	return []string{"empty"}
}

func (rowsMapEmptyStubRows) Close() error { return nil }

func (rowsMapEmptyStubRows) Next(dest []driver.Value) error {
	return io.EOF
}

type rowsMapScanErrStubRows struct {
	done bool
}

func (rowsMapScanErrStubRows) Columns() []string {
	return []string{"bad"}
}

func (rowsMapScanErrStubRows) Close() error { return nil }

func (r *rowsMapScanErrStubRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = struct{}{}
	r.done = true
	return nil
}

var rowsMapCloseCount atomic.Int32

type rowsMapTrackedCloseRows struct {
	rowsMapScanErrStubRows
}

func (rowsMapTrackedCloseRows) Columns() []string {
	return []string{"bad"}
}

func (r *rowsMapTrackedCloseRows) Close() error {
	rowsMapCloseCount.Add(1)
	return nil
}

func TestSQLRowsScanHelpers(t *testing.T) {
	db, err := sql.Open(rowsMapTestDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	gotRowsMap := (&SQLRows{rows: rows}).RowsMap()
	wantRowsMap := RowsMap{
		{"id": "7", "name": "alice"},
		{"id": "", "name": "bob"},
	}
	if !reflect.DeepEqual(gotRowsMap, wantRowsMap) {
		t.Fatalf("RowsMap() = %#v, want %#v", gotRowsMap, wantRowsMap)
	}

	rows, err = db.Query("SELECT * FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	gotRowsMapInterface := (&SQLRows{rows: rows}).RowsMapInterface()
	wantRowsMapInterface := RowsMapInterface{
		{"id": "7", "name": "alice"},
		{"id": "", "name": "bob"},
	}
	if !reflect.DeepEqual(gotRowsMapInterface, wantRowsMapInterface) {
		t.Fatalf("RowsMapInterface() = %#v, want %#v", gotRowsMapInterface, wantRowsMapInterface)
	}

	rows, err = db.Query("SELECT * FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	gotRowsMapNull := (&SQLRows{rows: rows}).RowsMapNull()
	wantRowsMapNull := RowsMapInterface{
		{"id": "7", "name": "alice"},
		{"id": nil, "name": "bob"},
	}
	if !reflect.DeepEqual(gotRowsMapNull, wantRowsMapNull) {
		t.Fatalf("RowsMapNull() = %#v, want %#v", gotRowsMapNull, wantRowsMapNull)
	}
}

func TestSQLRowsScanErrorBranches(t *testing.T) {
	db, err := sql.Open(rowsMapTestDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if got := (&SQLRows{rows: rows}).RowsMapInterface(); len(got) != 0 {
		t.Fatalf("RowsMapInterface closed rows = %#v, want empty", got)
	}
	if cols, data := (&SQLRows{rows: rows}).DoubleSlice(); len(cols) != 0 || len(data) != 0 {
		t.Fatalf("DoubleSlice closed rows = %#v, %#v; want empty", cols, data)
	}
	if cols, data := (&SQLRows{rows: rows}).TripleByte(); len(cols) != 0 || len(data) != 0 {
		t.Fatalf("TripleByte closed rows = %#v, %#v; want empty", cols, data)
	}

	rows, err = db.Query("SELECT scan_error FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if got := (&SQLRows{rows: rows}).RowsMap(); len(got) != 0 {
		t.Fatalf("RowsMap scan error = %#v; want empty", got)
	}

	rows, err = db.Query("SELECT scan_error FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if got := (&SQLRows{rows: rows}).RowsMapInterface(); len(got) != 0 {
		t.Fatalf("RowsMapInterface scan error = %#v; want empty", got)
	}

	rows, err = db.Query("SELECT scan_error FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if got := (&SQLRows{rows: rows}).RowsMapNull(); len(got) != 0 {
		t.Fatalf("RowsMapNull scan error = %#v; want empty", got)
	}

	rows, err = db.Query("SELECT scan_error FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if cols, data := (&SQLRows{rows: rows}).DoubleSlice(); len(cols) != 0 || len(data) != 0 {
		t.Fatalf("DoubleSlice scan error = %#v, %#v; want empty", cols, data)
	}

	rows, err = db.Query("SELECT scan_error FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	if cols, data := (&SQLRows{rows: rows}).TripleByte(); len(cols) != 0 || len(data) != 0 {
		t.Fatalf("TripleByte scan error = %#v, %#v; want empty", cols, data)
	}

	if got := (&SQLRows{err: errors.New("query failed")}).RowsMapNull(); len(got) != 0 {
		t.Fatalf("RowsMapNull error = %#v, want empty", got)
	}
}

func TestSQLRowsCloseOnScanError(t *testing.T) {
	db, err := sql.Open(rowsMapTestDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name string
		run  func(*SQLRows)
	}{
		{
			name: "RowsMap",
			run: func(r *SQLRows) {
				_ = r.RowsMap()
			},
		},
		{
			name: "RowsMapInterface",
			run: func(r *SQLRows) {
				_ = r.RowsMapInterface()
			},
		},
		{
			name: "RowsMapNull",
			run: func(r *SQLRows) {
				_ = r.RowsMapNull()
			},
		},
		{
			name: "DoubleSlice",
			run: func(r *SQLRows) {
				_, _ = r.DoubleSlice()
			},
		},
		{
			name: "TripleByte",
			run: func(r *SQLRows) {
				_, _ = r.TripleByte()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowsMapCloseCount.Store(0)
			rows, err := db.Query("SELECT tracked_scan_error FROM stub")
			if err != nil {
				t.Fatal(err)
			}
			tt.run(&SQLRows{rows: rows})
			if got := rowsMapCloseCount.Load(); got != 1 {
				t.Fatalf("close count = %d, want 1", got)
			}
		})
	}
}
