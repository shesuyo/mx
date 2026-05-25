package mx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
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

func newUnitDB(tables map[string][]string) *DataBase {
	tableColumns := make(map[string]Columns, len(tables))
	for tableName, names := range tables {
		cols := make(Columns, len(names))
		for _, name := range names {
			cols[name] = Column{Name: name}
		}
		tableColumns[tableName] = cols
	}
	return &DataBase{
		Schema:       "test_schema",
		tableColumns: tableColumns,
		mm:           new(sync.Mutex),
	}
}

func newUnitTableWithDB(tableName string, tables map[string][]string) *Table {
	db := newUnitDB(tables)
	return db.Table(tableName)
}

func assertParsed(t *testing.T, table *Table, wantQuery string, wantArgs []any) {
	t.Helper()
	gotQuery, gotArgs := table.Parse()
	if gotQuery != wantQuery {
		t.Fatalf("Parse query = %q, want %q", gotQuery, wantQuery)
	}
	if wantArgs == nil && len(gotArgs) == 0 {
		return
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("Parse args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestJoinConsHaveTable(t *testing.T) {
	joins := JoinCons{{TableName: "weapon"}, {TableName: "gem"}}
	if !joins.HaveTable("weapon") || joins.HaveTable("user") {
		t.Fatalf("HaveTable returned unexpected values")
	}
}

func TestSearchFieldsGroupHavingAndWarping(t *testing.T) {
	table := newUnitTableWithDB("user", map[string][]string{
		"user":   {"id", "name", "weapon_id", "created_at"},
		"weapon": {"id", "name"},
	})

	got := table.
		Fields("$C", "DISTINCT user.name AS user_name", "weapon.name AS weapon_name", "COUNT(*) AS total", "DATE_FORMAT(user.created_at,'%Y-%m-%d') AS dt").
		Group("user.name").
		Having("COUNT(*) > ?", 1).
		OrderBy("user.name").
		Limit(5)

	wantQuery := "SELECT COUNT(*) AS total,DISTINCT `user`.`name` AS user_name,`weapon`.`name` AS weapon_name,COUNT(*) AS total,DATE_FORMAT(user.created_at,'%Y-%m-%d') AS dt FROM `user` LEFT JOIN weapon ON user.weapon_id = weapon.id GROUP BY user.name HAVING COUNT(*) > ? ORDER BY user.name ASC LIMIT ?"
	wantArgs := []any{1, 5}
	assertParsed(t, got, wantQuery, wantArgs)
}

func TestSearchJoinsAutoDetectsRelationshipDirections(t *testing.T) {
	tests := []struct {
		name      string
		tables    map[string][]string
		wantQuery string
	}{
		{
			name: "join table has compact foreign key",
			tables: map[string][]string{
				"user":   {"id"},
				"weapon": {"id", "userid"},
			},
			wantQuery: "SELECT * FROM `user` LEFT JOIN weapon ON weapon.userid = user.id",
		},
		{
			name: "join table has underscored foreign key",
			tables: map[string][]string{
				"user":   {"id"},
				"weapon": {"id", "user_id"},
			},
			wantQuery: "SELECT * FROM `user` LEFT JOIN weapon ON weapon.user_id = user.id",
		},
		{
			name: "base table has compact foreign key",
			tables: map[string][]string{
				"user":   {"id", "weaponid"},
				"weapon": {"id"},
			},
			wantQuery: "SELECT * FROM `user` LEFT JOIN weapon ON user.weaponid = weapon.id",
		},
		{
			name: "base table has underscored foreign key",
			tables: map[string][]string{
				"user":   {"id", "weapon_id"},
				"weapon": {"id"},
			},
			wantQuery: "SELECT * FROM `user` LEFT JOIN weapon ON user.weapon_id = weapon.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newUnitTableWithDB("user", tt.tables)
			assertParsed(t, table.Joins("weapon"), tt.wantQuery, nil)
		})
	}
}

func TestSearchExplicitJoinTableNameAndFieldWrapping(t *testing.T) {
	table := newUnitTableWithDB("user", map[string][]string{
		"user":   {"id", "name"},
		"weapon": {"id", "name"},
	})

	table.Search.TableName("member")
	if table.Search.tableName != "member" {
		t.Fatalf("TableName did not update search table name")
	}
	table.Search.TableName("user")

	assertParsed(
		t,
		table.Joins("weapon", "weapon.user_id = user.id").Fields("`user`.`id`", ".name", "weapon.", "missing.field", "COUNT(1)", "name alias"),
		"SELECT `user`.`id`,`user`.`name`,weapon.,missing.field,COUNT(1),name alias FROM `user` LEFT JOIN weapon ON weapon.user_id = user.id",
		nil,
	)
}

func TestTableWhereHelpersBuildExpectedSQL(t *testing.T) {
	table := newUnitTableWithDB("user", map[string][]string{
		"user": {"id", "name", "created_at", "age", IsDeleted},
	})

	tests := []struct {
		name      string
		table     *Table
		wantQuery string
		wantArgs  []any
	}{
		{
			name:      "WhereNotEmpty empty keeps original",
			table:     table.WhereNotEmpty("name = ?", ""),
			wantQuery: "SELECT * FROM `user` WHERE user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereNotEmpty value",
			table:     table.WhereNotEmpty("name = ?", "alice"),
			wantQuery: "SELECT * FROM `user` WHERE name = ? AND user.is_deleted = ?",
			wantArgs:  []any{"alice", 0},
		},
		{
			name:      "WhereTime empty",
			table:     table.WhereTime("created_at", Time{}),
			wantQuery: "SELECT * FROM `user` WHERE user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereTime day and time",
			table:     table.WhereTime("created_at", Time{Day: "2026-05-13", Stime: "08:00", Etime: "10:00"}),
			wantQuery: "SELECT * FROM `user` WHERE created_at >= ? AND created_at <= ? AND DATE_FORMAT(created_at,'%H:%i') >= ? AND DATE_FORMAT(created_at,'%H:%i') <= ? AND user.is_deleted = ?",
			wantArgs:  []any{"2026-05-13 00:00:00", "2026-05-13 23:59:59", "08:00", "10:00", 0},
		},
		{
			name:      "WherePeriod valid",
			table:     table.WherePeriod("created_at", "2026-05", ""),
			wantQuery: "SELECT * FROM `user` WHERE created_at >= ? AND created_at < ? AND user.is_deleted = ?",
			wantArgs:  []any{"2026-05-01 00:00:00", "2026-06-01 00:00:00", 0},
		},
		{
			name:      "WherePeriod invalid",
			table:     table.WherePeriod("created_at", "", ""),
			wantQuery: "SELECT * FROM `user` WHERE user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereStartEndDay",
			table:     table.WhereStartEndDay("created_at", "2026-05-13", ""),
			wantQuery: "SELECT * FROM `user` WHERE created_at >= ? AND created_at <= ? AND user.is_deleted = ?",
			wantArgs:  []any{"2026-05-13 00:00:00", "2026-05-13 23:59:59", 0},
		},
		{
			name:      "WhereStartEndMonth",
			table:     table.WhereStartEndMonth("created_at", "2026-05", ""),
			wantQuery: "SELECT * FROM `user` WHERE DATE_FORMAT(created_at,'%Y-%m') >= ? AND DATE_FORMAT(created_at,'%Y-%m') <= ? AND user.is_deleted = ?",
			wantArgs:  []any{"2026-05", "2026-05", 0},
		},
		{
			name:      "WhereStartEndTime",
			table:     table.WhereStartEndTime("created_at", "08:30", ""),
			wantQuery: "SELECT * FROM `user` WHERE DATE_FORMAT(created_at,'%H:%i') >= ? AND DATE_FORMAT(created_at,'%H:%i') <= ? AND user.is_deleted = ?",
			wantArgs:  []any{"08:30", "08:30", 0},
		},
		{
			name:      "WhereDay",
			table:     table.WhereDay("created_at", "2026-05-13"),
			wantQuery: "SELECT * FROM `user` WHERE (created_at >= '2026-05-13 00:00:00' AND created_at < '2026-05-14 00:00:00') AND user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereMonth",
			table:     table.WhereMonth("created_at", "2026-05"),
			wantQuery: "SELECT * FROM `user` WHERE (created_at >= '2026-05-01 00:00:00' AND created_at < '2026-06-01 00:00:00') AND user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereLike empty",
			table:     table.WhereLike("name", ""),
			wantQuery: "SELECT * FROM `user` WHERE user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "WhereLike",
			table:     table.WhereLike("name", "ali"),
			wantQuery: "SELECT * FROM `user` WHERE name LIKE ? AND user.is_deleted = ?",
			wantArgs:  []any{"%ali%", 0},
		},
		{
			name:      "WhereLikeLeft",
			table:     table.WhereLikeLeft("name", "ali"),
			wantQuery: "SELECT * FROM `user` WHERE name LIKE ? AND user.is_deleted = ?",
			wantArgs:  []any{"%ali", 0},
		},
		{
			name:      "WhereLikeRight",
			table:     table.WhereLikeRight("name", "ali"),
			wantQuery: "SELECT * FROM `user` WHERE name LIKE ? AND user.is_deleted = ?",
			wantArgs:  []any{"ali%", 0},
		},
		{
			name:      "InWhere",
			table:     table.InWhere("age", 9, 3, 7),
			wantQuery: "SELECT * FROM `user` WHERE age >=? AND age <=? AND user.is_deleted = ?",
			wantArgs:  []any{3, 9, 0},
		},
		{
			name:      "Page clamps negative offset",
			table:     table.Page(20, 0),
			wantQuery: "SELECT * FROM `user` WHERE user.is_deleted = ? LIMIT ? OFFSET ?",
			wantArgs:  []any{0, 20, 0},
		},
		{
			name:      "FieldCount custom alias first token",
			table:     table.FieldCount("num ignored"),
			wantQuery: "SELECT COUNT(*) AS num FROM `user` WHERE user.is_deleted = ?",
			wantArgs:  []any{0},
		},
		{
			name:      "Group Having",
			table:     table.Fields("age").Group("age").Having("COUNT(*) > ?", 2),
			wantQuery: "SELECT `user`.`age` FROM `user` WHERE user.is_deleted = ? GROUP BY age HAVING COUNT(*) > ?",
			wantArgs:  []any{0, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParsed(t, tt.table, tt.wantQuery, tt.wantArgs)
		})
	}
}

func TestTableSimpleHelpersAndDatabaseMetadata(t *testing.T) {
	db := newUnitDB(map[string][]string{
		"user": {"id", "name"},
	})

	if !db.HaveTable("user") || db.HaveTable("missing") {
		t.Fatalf("HaveTable returned unexpected values")
	}
	if !db.haveTablename("user") || db.haveTablename("missing") {
		t.Fatalf("haveTablename returned unexpected values")
	}
	if got := db.getColumns("user"); !got.HaveColumn("name") {
		t.Fatalf("getColumns cached user columns = %#v", got)
	}
	if got := db.Table("user"); got.Name() != "user" || !got.HaveColumn("id") || got.Search.table != got {
		t.Fatalf("Table(user) = %#v", got)
	}
	if got := db.Debug(true); got != db || !db.debug {
		t.Fatalf("Debug(true) did not update db")
	}
	db.Log("covered")
	db.LogSQL("SELECT ?", 1)
	db.Debug(false)

	idxs := Indexs{{NonUnique: 1}, {NonUnique: 0}}
	if idxs.IsUnique() {
		t.Fatalf("IsUnique with mixed indexes = true, want false because first index is non-unique")
	}
	if !(Indexs{{NonUnique: 0}, {NonUnique: 1}}).IsUnique() {
		t.Fatalf("IsUnique returns based on first index; expected true")
	}
	if (Indexs{}).IsUnique() {
		t.Fatalf("empty IsUnique = true, want false")
	}
}

type customDBNameForTest struct {
	ID int64
}

func (customDBNameForTest) DBName() string {
	return "custom_table"
}

func TestGetStructDBNameAndID(t *testing.T) {
	type PlainUser struct {
		ID int64
	}
	custom := customDBNameForTest{ID: 99}
	if got := getStructDBName(reflect.ValueOf(custom)); got != "custom_table" {
		t.Fatalf("getStructDBName custom = %q, want custom_table", got)
	}
	if got := getStructDBName(reflect.ValueOf(PlainUser{})); got != "plain_user" {
		t.Fatalf("getStructDBName plain = %q, want plain_user", got)
	}
	if got := getStructID(reflect.ValueOf(&custom)); got != 99 {
		t.Fatalf("getStructID custom = %d, want 99", got)
	}
	if got := getStructID(reflect.ValueOf(struct{ Name string }{})); got != 0 {
		t.Fatalf("getStructID missing = %d, want 0", got)
	}
}

const miniDBDriverName = "mx_minidb_stub_test"

func init() {
	sql.Register(miniDBDriverName, miniDBDriver{})
}

type miniDBDriver struct{}

func (miniDBDriver) Open(name string) (driver.Conn, error) {
	return miniDBConn{}, nil
}

type miniDBConn struct{}

func (miniDBConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (miniDBConn) Close() error { return nil }
func (miniDBConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}
func (miniDBConn) Ping(ctx context.Context) error {
	return nil
}
func (miniDBConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch {
	case query == "SELECT DATABASE()":
		return &miniRows{cols: []string{"DATABASE()"}, rows: [][]driver.Value{{[]byte("unit_schema")}}}, nil
	case strings.Contains(query, "information_schema.`COLUMNS`"):
		return &miniRows{cols: []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "COLUMN_COMMENT", "COLUMN_TYPE", "DATA_TYPE", "IS_NULLABLE"}, rows: [][]driver.Value{
			{[]byte("unit_schema"), []byte("user"), []byte("id"), []byte(""), []byte("int"), []byte("int"), []byte("NO")},
			{[]byte("unit_schema"), []byte("user"), []byte("name"), []byte(""), []byte("varchar(20)"), []byte("varchar"), []byte("YES")},
		}}, nil
	default:
		return &miniRows{cols: []string{"v"}, rows: [][]driver.Value{{[]byte("1")}}}, nil
	}
}

type miniRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *miniRows) Columns() []string { return r.cols }
func (r *miniRows) Close() error      { return nil }
func (r *miniRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func TestCheckDBInitAndDBAccessor(t *testing.T) {
	raw, err := sql.Open(miniDBDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	conf := Config{Timeout: time.Second, MaxIdleConns: 2, MaxOpenConns: 3}
	if err := checkDBInit(raw, conf); err != nil {
		t.Fatalf("checkDBInit() error = %v", err)
	}
	if err := checkDBInitContext(raw, conf); err != nil {
		t.Fatalf("checkDBInitContext() error = %v", err)
	}

	db := &DataBase{db: raw}
	if db.DB() != raw {
		t.Fatalf("DB() did not return raw handle")
	}
}
