package mx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

const edgeSlowPingDriverName = "mx_edge_slow_ping_stub_test"

var edgeHookErr = errors.New("edge hook failed")

func init() {
	sql.Register(edgeSlowPingDriverName, edgeSlowPingDriver{})
}

type edgeSlowPingDriver struct{}

func (edgeSlowPingDriver) Open(name string) (driver.Conn, error) {
	return edgeSlowPingConn{}, nil
}

type edgeSlowPingConn struct{}

func (edgeSlowPingConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (edgeSlowPingConn) Close() error { return nil }

func (edgeSlowPingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (edgeSlowPingConn) Ping(ctx context.Context) error {
	time.Sleep(25 * time.Millisecond)
	return nil
}

type edgeSaveHookModel struct {
	ID               int    `mx:"id"`
	Name             string `mx:"name"`
	FailBeforeCreate bool   `mx:"-"`
	FailBeforeUpdate bool   `mx:"-"`
	FailAfterUpdate  bool   `mx:"-"`
}

func (edgeSaveHookModel) DBName() string { return "user" }

func (m *edgeSaveHookModel) BeforeCreate() error {
	if m.FailBeforeCreate {
		return edgeHookErr
	}
	return nil
}

func (m *edgeSaveHookModel) BeforeUpdate() error {
	if m.FailBeforeUpdate {
		return edgeHookErr
	}
	return nil
}

func (m *edgeSaveHookModel) AfterUpdate() error {
	if m.FailAfterUpdate {
		return edgeHookErr
	}
	return nil
}

func TestEdgeTableSaveHookAndExecErrors(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	// Save 创建路径会先执行 BeforeCreate，钩子失败时不应该继续写库。
	if _, err := table.Save(&edgeSaveHookModel{Name: "bad", FailBeforeCreate: true}); !errors.Is(err, edgeHookErr) {
		t.Fatalf("Save before create error = %v, want %v", err, edgeHookErr)
	}

	// 更新路径的前置和后置钩子都要把错误向调用方透出。
	if _, err := table.Save(&edgeSaveHookModel{ID: 1, Name: "bad", FailBeforeUpdate: true}); !errors.Is(err, edgeHookErr) {
		t.Fatalf("Save before update error = %v, want %v", err, edgeHookErr)
	}
	if _, err := table.Save(&edgeSaveHookModel{ID: 1, Name: "bad", FailAfterUpdate: true}); !errors.Is(err, edgeHookErr) {
		t.Fatalf("Save after update error = %v, want %v", err, edgeHookErr)
	}

	crudMu.Lock()
	crudExecErr = errors.New("save update failed")
	crudMu.Unlock()
	if _, err := table.Save(&edgeSaveHookModel{ID: 1, Name: "exec fail"}); !errors.Is(err, ErrExec) {
		t.Fatalf("Save update exec error = %v, want ErrExec", err)
	}

	crudMu.Lock()
	crudExecErr = nil
	crudMu.Unlock()
	models := []edgeSaveHookModel{{Name: "ok"}, {Name: "bad", FailBeforeCreate: true}}
	if _, err := table.Save(&models); !errors.Is(err, edgeHookErr) {
		t.Fatalf("Save slice stops on hook error = %v, want %v", err, edgeHookErr)
	}
}

func TestEdgeDataBaseStructCRUDErrors(t *testing.T) {
	db := resetCRUDStubDB(t)

	// DataBase.Create 应直接返回结构体钩子的错误。
	if _, err := db.Create(&edgeSaveHookModel{Name: "bad", FailBeforeCreate: true}); !errors.Is(err, edgeHookErr) {
		t.Fatalf("DataBase.Create hook error = %v, want %v", err, edgeHookErr)
	}

	crudMu.Lock()
	crudExecErr = errors.New("create failed")
	crudMu.Unlock()
	ids, err := db.Creates(&[]edgeSaveHookModel{{Name: "bad"}})
	if !errors.Is(err, ErrExec) || len(ids) != 0 {
		t.Fatalf("DataBase.Creates exec error ids=%#v err=%v, want empty ids and ErrExec", ids, err)
	}

	crudMu.Lock()
	crudExecErr = nil
	crudMu.Unlock()
	affected, err := db.Deletes(&[]edgeSaveHookModel{{ID: 1}, {Name: "missing id"}})
	if !errors.Is(err, ErrMustNeedID) || affected != 2 {
		t.Fatalf("DataBase.Deletes partial error affected=%d err=%v, want 2 and ErrMustNeedID", affected, err)
	}

	if err := db.Update(&edgeSaveHookModel{Name: "missing id"}); !errors.Is(err, ErrNoUpdateKey) {
		t.Fatalf("DataBase.Update missing id error = %v, want ErrNoUpdateKey", err)
	}
	updated, err := db.Updates(&[]edgeSaveHookModel{{ID: 1, Name: "ok"}, {Name: "missing id"}})
	if !errors.Is(err, ErrNoUpdateKey) || updated != 0 {
		t.Fatalf("DataBase.Updates stops on error updated=%d err=%v, want 0 and ErrNoUpdateKey", updated, err)
	}

	// 当前 Find 在无参数时会先访问 args[0]，这里锁定这个边界行为，便于后续修复时有测试提醒。
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("DataBase.Find without args did not panic")
			}
		}()
		var target edgeSaveHookModel
		_ = db.Find(&target)
	}()
}

func TestEdgeTableMetadataCreateOrUpdateAndFullMemberErrors(t *testing.T) {
	db := resetCRUDStubDB(t)

	crudMu.Lock()
	crudCountValue = "1"
	crudMu.Unlock()
	ghost := db.Table("ghost")
	if ghost.Name() != "ghost" || ghost.Search == nil || ghost.Search.table != ghost {
		t.Fatalf("Table missing metadata = %#v", ghost)
	}
	crudMu.Lock()
	queries := append([]string(nil), crudQueries...)
	crudMu.Unlock()
	sawTableCheck := false
	for _, query := range queries {
		if strings.Contains(query, "information_schema.tables") {
			sawTableCheck = true
			break
		}
	}
	if !sawTableCheck {
		t.Fatalf("Table missing metadata queries = %#v, want information_schema.tables check", queries)
	}

	table := db.Table("user")
	crudMu.Lock()
	crudCountValue = "1"
	crudExecErr = errors.New("update failed")
	crudMu.Unlock()
	err := table.CreateOrUpdate(map[string]any{"name": "same", "age": 31}, "name")
	if !errors.Is(err, ErrExec) {
		t.Fatalf("CreateOrUpdate update error = %v, want ErrExec", err)
	}

	crudMu.Lock()
	crudCountValue = "0"
	crudExecErr = errors.New("full member failed")
	crudMu.Unlock()
	err = table.FullMember([]map[string]string{{"name": "alice"}}, "group_id", "g1", "name")
	if err == nil || err.Error() != "full member failed" {
		t.Fatalf("FullMember delete error = %v, want full member failed", err)
	}
}

func TestEdgeReflectScannerTimeAndBytesBranches(t *testing.T) {
	// 不可取地址的值不具备 Scanner 能力，应返回未处理且无错误。
	handled, err := scanReflectValue(reflect.ValueOf(1), []byte("1"))
	if handled || err != nil {
		t.Fatalf("scanReflectValue unaddressable handled=%v err=%v, want false nil", handled, err)
	}

	var nullString sql.NullString
	handled, err = scanReflectValue(reflect.ValueOf(&nullString).Elem(), []byte("alice"))
	if !handled || err != nil || !nullString.Valid || nullString.String != "alice" {
		t.Fatalf("scanReflectValue NullString handled=%v value=%#v err=%v", handled, nullString, err)
	}

	var nullTime sql.NullTime
	handled, err = scanReflectValue(reflect.ValueOf(&nullTime).Elem(), []byte("not-time"))
	if !handled || err == nil {
		t.Fatalf("scanReflectValue bad NullTime handled=%v err=%v, want handled error", handled, err)
	}

	// 带引号的空时间和零日期都应该被当成 time.Time 零值。
	for _, raw := range [][]byte{[]byte(`""`), []byte(`"0000-00-00"`)} {
		got, err := parseReflectTime(raw)
		if err != nil || !got.IsZero() {
			t.Fatalf("parseReflectTime(%q) = %v, %v; want zero nil", raw, got, err)
		}
	}

	type bytesAlias []byte
	var alias bytesAlias
	if err := setReflectBytes(reflect.ValueOf(&alias).Elem(), []byte("abc")); err != nil {
		t.Fatalf("setReflectBytes alias error = %v", err)
	}
	if string(alias) != "abc" {
		t.Fatalf("setReflectBytes alias = %q, want abc", string(alias))
	}
}

func TestEdgeSearchAndRowMapSmallBranches(t *testing.T) {
	table := newUnitTableWithDB("user", map[string][]string{
		"user": {"id", "name", "age", "created_at"},
	})

	// In 传入切片时会展开参数；传入空切片则保持原查询不变。
	assertParsed(t, table.In("id", []int{1, 2}), "SELECT * FROM `user` WHERE id IN (?,?)", []any{1, 2})
	assertParsed(t, table.In("id", []string{}), "SELECT * FROM `user`", nil)
	if got := table.WhereStartEndDay("created_at", "", ""); got != table {
		t.Fatalf("WhereStartEndDay empty returned different table")
	}
	if got := table.WhereStartEndMonth("created_at", "", ""); got != table {
		t.Fatalf("WhereStartEndMonth empty returned different table")
	}
	if got := table.WhereStartEndTime("created_at", "", ""); got != table {
		t.Fatalf("WhereStartEndTime empty returned different table")
	}
	assertParsed(t, table.FieldCount(), "SELECT COUNT(*) AS total FROM `user`", nil)

	row := RowMap{"bad": "abc", "off": "0"}
	if got := row.Int32("bad"); got != 0 {
		t.Fatalf("RowMap.Int32 bad = %d, want 0", got)
	}
	if row.Bool("off") {
		t.Fatalf("RowMap.Bool off = true, want false")
	}
	if got := (RowsMapInterface{{"name": "a"}, {"name": "b"}}).Filter("name", "a"); len(got) != 1 || got[0].String("name") != "a" {
		t.Fatalf("RowsMapInterface.Filter string equal = %#v", got)
	}
}

func TestEdgeSQLRowsFindTagsAndToStructErrors(t *testing.T) {
	db := openRowsMapStubDB(t)
	defer db.Close()

	queryRows := func(t *testing.T) *SQLRows {
		t.Helper()
		rows, err := db.Query("SELECT * FROM stub")
		if err != nil {
			t.Fatal(err)
		}
		return &SQLRows{rows: rows}
	}

	type taggedRow struct {
		Identifier int    `dbname:"id"`
		Display    string `dbname:"name"`
	}

	// Find 的 dbname 标签路径需要覆盖，避免只测默认命名映射。
	var one taggedRow
	if err := queryRows(t).Find(&one); err != nil {
		t.Fatalf("Find tagged struct error = %v", err)
	}
	if one.Identifier != 7 || one.Display != "alice" {
		t.Fatalf("Find tagged struct = %#v", one)
	}

	var many []taggedRow
	if err := queryRows(t).Find(&many); err != nil {
		t.Fatalf("Find tagged slice error = %v", err)
	}
	if len(many) != 2 || many[0].Identifier != 7 || many[1].Display != "bob" {
		t.Fatalf("Find tagged slice = %#v", many)
	}

	// 当前 int64 标量会进入 SQLRows.setValue 的默认分支并 panic，先锁定现有行为。
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("Find int64 did not panic")
			}
		}()
		var id64 int64
		_ = queryRows(t).Find(&id64)
	}()

	if err := queryRows(t).ToStruct(taggedRow{}); err == nil || !strings.Contains(err.Error(), "addr") {
		t.Fatalf("SQLRows.ToStruct non-pointer error = %v, want addr error", err)
	}
}

func TestEdgeTableStructNewModelErrorsAndInitTimeout(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	// Table.Struct/ToStruct 需要把 NewModelStruct 的非指针错误透传出来。
	if err := table.Struct(afterFindModel{}); err == nil || !strings.Contains(err.Error(), "addr") {
		t.Fatalf("Table.Struct non-pointer error = %v, want addr error", err)
	}
	if err := table.ToStruct(afterFindModel{}); err == nil || !strings.Contains(err.Error(), "addr") {
		t.Fatalf("Table.ToStruct non-pointer error = %v, want addr error", err)
	}

	raw, err := sql.Open(edgeSlowPingDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	start := time.Now()
	if err := checkDBInit(raw, Config{Timeout: time.Millisecond}); err == nil {
		t.Fatalf("checkDBInit timeout error = nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("checkDBInit timeout took too long: %v", elapsed)
	}
}

func TestEdgeDMRowsMapTimestampAndByteColumn(t *testing.T) {
	db, err := sql.Open(edgeDMDefaultDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT dm defaults")
	if err != nil {
		t.Fatal(err)
	}

	// 达梦适配需要同时处理时间列和普通字节列，并把列名统一转成小写。
	got := (&SQLRows{driver: "dm", rows: rows}).RowsMap()
	want := RowsMap{{
		"created_at": "2026-05-14 09:08:07",
		"note":       "ok",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dm default time rows = %#v, want %#v", got, want)
	}
}

const edgeDMDefaultDriverName = "mx_edge_dm_default_stub_test"

func init() {
	sql.Register(edgeDMDefaultDriverName, edgeDMDefaultDriver{})
}

type edgeDMDefaultDriver struct{}

func (edgeDMDefaultDriver) Open(name string) (driver.Conn, error) {
	return edgeDMDefaultConn{}, nil
}

type edgeDMDefaultConn struct{}

func (edgeDMDefaultConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (edgeDMDefaultConn) Close() error { return nil }

func (edgeDMDefaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (edgeDMDefaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return &edgeDMDefaultRows{
		cols:  []string{"CREATED_AT", "NOTE"},
		types: []string{"TIMESTAMP", "VARCHAR"},
		rows: [][]driver.Value{{
			time.Date(2026, 5, 14, 9, 8, 7, 0, time.Local),
			[]byte("ok"),
		}},
	}, nil
}

type edgeDMDefaultRows struct {
	cols  []string
	types []string
	rows  [][]driver.Value
	idx   int
}

func (r *edgeDMDefaultRows) Columns() []string { return r.cols }
func (r *edgeDMDefaultRows) Close() error      { return nil }

func (r *edgeDMDefaultRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func (r *edgeDMDefaultRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}
