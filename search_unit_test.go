package mx

import (
	"reflect"
	"testing"
)

func newUnitTable(tableName string, columns ...string) *Table {
	cols := make(Columns, len(columns))
	for _, column := range columns {
		cols[column] = Column{Name: column}
	}
	db := &DataBase{
		tableColumns: map[string]Columns{
			tableName: cols,
		},
	}
	table := &Table{
		DataBase:  db,
		tableName: tableName,
		Columns:   cols,
	}
	table.Search = &Search{
		table:     table,
		tableName: tableName,
	}
	return table
}

func TestSearchParseBuildsExpectedSQL(t *testing.T) {
	table := newUnitTable("user", "id", "name", "age")

	query, args := table.
		Fields("id", "name").
		Where("age > ?", 18).
		In("id", []int{1, 2}).
		OrderBy("age", true).
		Limit(10).
		Offset(20).
		Parse()

	wantQuery := "SELECT `user`.`id`,`user`.`name` FROM `user` WHERE age > ? AND id IN (?,?) ORDER BY age DESC LIMIT ? OFFSET ?"
	wantArgs := []any{18, 1, 2, 10, 20}
	if query != wantQuery {
		t.Fatalf("Parse query = %q, want %q", query, wantQuery)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("Parse args = %#v, want %#v", args, wantArgs)
	}
}

func TestSearchParseAddsSoftDeleteCondition(t *testing.T) {
	table := newUnitTable("user", "id", IsDeleted)

	query, args := table.WhereID(7).Parse()

	wantQuery := "SELECT * FROM `user` WHERE user.id = ? AND user.is_deleted = ?"
	wantArgs := []any{7, 0}
	if query != wantQuery {
		t.Fatalf("Parse query = %q, want %q", query, wantQuery)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("Parse args = %#v, want %#v", args, wantArgs)
	}
}

func TestSearchParseIsStableAcrossCalls(t *testing.T) {
	table := newUnitTable("user", "id", IsDeleted)
	search := table.Where("id = ?", 7).Search

	query1, args1 := search.Parse()
	query2, args2 := search.Parse()

	wantQuery := "SELECT * FROM `user` WHERE id = ? AND user.is_deleted = ?"
	wantArgs := []any{7, 0}
	if query1 != wantQuery {
		t.Fatalf("first Parse query = %q, want %q", query1, wantQuery)
	}
	if query2 != wantQuery {
		t.Fatalf("second Parse query = %q, want %q", query2, wantQuery)
	}
	if !reflect.DeepEqual(args1, wantArgs) {
		t.Fatalf("first Parse args = %#v, want %#v", args1, wantArgs)
	}
	if !reflect.DeepEqual(args2, wantArgs) {
		t.Fatalf("second Parse args = %#v, want %#v", args2, wantArgs)
	}
}

func TestSearchInAndNotInIgnoreEmptyArgs(t *testing.T) {
	table := newUnitTable("user", "id")

	query, args := table.In("id").NotIn("id", []int{}).Parse()

	wantQuery := "SELECT * FROM `user`"
	if query != wantQuery {
		t.Fatalf("Parse query = %q, want %q", query, wantQuery)
	}
	if len(args) != 0 {
		t.Fatalf("Parse args = %#v, want empty", args)
	}
}

func TestSearchMustInShortCircuitsEmptyArgs(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "no args"},
		{name: "empty int slice", args: []any{[]int{}}},
		{name: "nil arg", args: []any{nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newUnitTable("user", "id")

			got := table.MustIn("id", tt.args...)
			if !got.Search.noNeedQuery {
				t.Fatal("MustIn should mark empty inputs as noNeedQuery")
			}
			if rows := got.RowsMap(); len(rows) != 0 {
				t.Fatalf("RowsMap length = %d, want 0", len(rows))
			}

			query, args := got.Parse()
			wantQuery := "SELECT * FROM `user`"
			if query != wantQuery {
				t.Fatalf("Parse query = %q, want %q", query, wantQuery)
			}
			if len(args) != 0 {
				t.Fatalf("Parse args = %#v, want empty", args)
			}
		})
	}
}

func TestSearchMustInExpandsNonEmptySlice(t *testing.T) {
	table := newUnitTable("user", "id")

	got := table.MustIn("id", []int{1, 2, 3})
	if got.Search.noNeedQuery {
		t.Fatal("MustIn should query when slice has values")
	}

	query, args := got.Parse()
	wantQuery := "SELECT * FROM `user` WHERE id IN (?,?,?)"
	wantArgs := []any{1, 2, 3}
	if query != wantQuery {
		t.Fatalf("Parse query = %q, want %q", query, wantQuery)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("Parse args = %#v, want %#v", args, wantArgs)
	}
}

func TestTableCloneKeepsSearchStateIndependent(t *testing.T) {
	table := newUnitTable("user", "id")

	filtered := table.Where("id = ?", 1)
	baseQuery, baseArgs := table.Parse()
	filteredQuery, filteredArgs := filtered.Parse()

	if baseQuery != "SELECT * FROM `user`" {
		t.Fatalf("base Parse query = %q, want unchanged base query", baseQuery)
	}
	if len(baseArgs) != 0 {
		t.Fatalf("base Parse args = %#v, want empty", baseArgs)
	}
	if filteredQuery != "SELECT * FROM `user` WHERE id = ?" {
		t.Fatalf("filtered Parse query = %q, want filtered query", filteredQuery)
	}
	if !reflect.DeepEqual(filteredArgs, []any{1}) {
		t.Fatalf("filtered Parse args = %#v, want %#v", filteredArgs, []any{1})
	}
}
