package mx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
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
	if query == "SELECT single FROM stub" {
		return &rowsMapSingleStubRows{}, nil
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
