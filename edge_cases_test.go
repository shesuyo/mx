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
	if !errors.Is(err, ErrNoUpdateKey) || updated != 1 {
		t.Fatalf("DataBase.Updates stops on error updated=%d err=%v, want 1 and ErrNoUpdateKey", updated, err)
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

type extraEmbedded struct {
	Embedded string `json:"embedded"`
}

type extraTaggedModel struct {
	extraEmbedded
	ID      int
	Alias   string `mx:"alias"`
	JSON    string `json:"json_name,omitempty"`
	Ignored string `mx:"-"`
}

func TestExtraStructMappingAndUnsupportedTargets(t *testing.T) {
	cols := map[string]int{"id": 0, "embedded": 1, "alias": 2, "json_name": 3}
	data := [][]byte{[]byte("5"), []byte("embed"), []byte("alias"), []byte("json")}

	// 底层 setStruct 需要同时处理匿名字段、mx 标签、json 标签和忽略字段。
	var one extraTaggedModel
	if err := setStruct(reflect.ValueOf(&one).Elem(), reflect.TypeOf(one), cols, data); err != nil {
		t.Fatalf("setStruct() error = %v", err)
	}
	if one.ID != 5 || one.Embedded != "embed" || one.Alias != "alias" || one.JSON != "json" || one.Ignored != "" {
		t.Fatalf("setStruct() model = %#v", one)
	}

	var many []extraTaggedModel
	if err := setSlice(reflect.ValueOf(&many).Elem(), reflect.TypeOf(many), cols, [][][]byte{data}); err != nil {
		t.Fatalf("setSlice() error = %v", err)
	}
	if len(many) != 1 || many[0].ID != 5 || many[0].Embedded != "embed" || many[0].Alias != "alias" {
		t.Fatalf("setSlice() = %#v", many)
	}

	// 无法转换的字段值要把错误向上传递，避免悄悄写入半成品对象。
	badData := [][]byte{[]byte("not-int"), []byte("embed"), []byte("alias"), []byte("json")}
	if err := setStruct(reflect.ValueOf(&one).Elem(), reflect.TypeOf(one), cols, badData); err == nil {
		t.Fatalf("setStruct() invalid int error = nil")
	}
	if err := setSlice(reflect.ValueOf(&many).Elem(), reflect.TypeOf(many), cols, [][][]byte{badData}); err == nil {
		t.Fatalf("setSlice() invalid int error = nil")
	}

	if _, err := NewModelStruct(extraTaggedModel{}); err == nil {
		t.Fatalf("NewModelStruct non-pointer error = nil")
	}
	if _, err := NewModelStruct((*extraTaggedModel)(nil)); err == nil {
		t.Fatalf("NewModelStruct nil pointer error = nil")
	}
}

func TestExtraSearchEmptyAndScalarBranches(t *testing.T) {
	db := resetCRUDStubDB(t)
	db.tableColumns["empty"] = Columns{
		"id":   {Name: "id"},
		"name": {Name: "name"},
	}
	table := db.Table("empty")

	// 空结果时 Search 的标量快捷方法都应该返回零值。
	if got := table.Fields("id").Int(); got != 0 {
		t.Fatalf("Search.Int empty = %d, want 0", got)
	}
	if got := table.Fields("name").String(); got != "" {
		t.Fatalf("Search.String empty = %q, want empty", got)
	}
	if got := table.Fields("id").Float(); got != 0 {
		t.Fatalf("Search.Float empty = %v, want 0", got)
	}

	userTable := db.Table("user")
	if got := userTable.Fields("age").Int(); got != 30 {
		t.Fatalf("Search.Int first field = %d, want 30", got)
	}
	if got := userTable.Fields("amount").Float(); got != 2.5 {
		t.Fatalf("Search.Float first field = %v, want 2.5", got)
	}

	// Fields 的空输入和 $c 别名分支都只影响解析，不应该触发查询。
	if got := userTable.Search.Clone().Fields(); got.fields != nil {
		t.Fatalf("Search.Fields empty mutated fields = %#v", got.fields)
	}
	query, _ := userTable.Fields("$c").Parse()
	if !strings.Contains(query, "COUNT(*) AS total") {
		t.Fatalf("Fields($c) query = %q", query)
	}
}

func TestExtraRowsNilAndQueryErrorBranches(t *testing.T) {
	// SQLRows 持有 nil rows 时，已显式防御的读取函数应安全返回空集合。
	nilRows := &SQLRows{}
	if got := nilRows.RowsMap(); len(got) != 0 {
		t.Fatalf("RowsMap nil rows = %#v", got)
	}
	if got := nilRows.RowsMapNull(); len(got) != 0 {
		t.Fatalf("RowsMapNull nil rows = %#v", got)
	}
	// RowsMapInterface 当前没有 nil rows 防御，这里锁定现有 panic 行为。
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("RowsMapInterface nil rows did not panic")
			}
		}()
		_ = nilRows.RowsMapInterface()
	}()

	db := resetCRUDStubDB(t)
	wantErr := errors.New("query failed")
	crudMu.Lock()
	crudQueryErr = wantErr
	crudMu.Unlock()

	log := &extraLogger{}
	db.log = log
	rows := db.Query("SELECT * FROM user")
	if !errors.Is(rows.err, wantErr) {
		t.Fatalf("DataBase.Query err = %v, want %v", rows.err, wantErr)
	}
	if len(log.logs) != 1 || len(log.errs) != 1 || log.logs[0].Way != QueryWay || !errors.Is(log.errs[0].Err, wantErr) {
		t.Fatalf("logger query events logs=%#v errs=%#v", log.logs, log.errs)
	}

	crudMu.Lock()
	crudQueryErr = nil
	crudExecErr = wantErr
	crudMu.Unlock()
	result := db.Exec("UPDATE user SET name=?", "x")
	if _, err := result.RowsAffected(); !errors.Is(err, wantErr) {
		t.Fatalf("DataBase.Exec err = %v, want %v", err, wantErr)
	}
	if len(log.logs) != 2 || len(log.errs) != 2 || log.logs[1].Way != ExecWay || !errors.Is(log.errs[1].Err, wantErr) {
		t.Fatalf("logger exec events logs=%#v errs=%#v", log.logs, log.errs)
	}
}

func TestExtraSQLRowsScanAndFindEdges(t *testing.T) {
	db := openRowsMapStubDB(t)
	defer db.Close()

	// 没有下一行时 Scan 返回 nil，并保持目标变量原值。
	rows, err := db.Query("SELECT empty FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	target := "keep"
	if err := (&SQLRows{rows: rows}).Scan(&target); err != nil {
		t.Fatalf("Scan empty error = %v", err)
	}
	if target != "keep" {
		t.Fatalf("Scan empty target = %q, want keep", target)
	}

	rows, err = db.Query("SELECT * FROM stub")
	if err != nil {
		t.Fatal(err)
	}
	var foundFloat float64
	if err := (&SQLRows{rows: rows}).Find(&foundFloat); err != nil {
		t.Fatalf("Find float error = %v", err)
	}
	if foundFloat != 0 {
		t.Fatalf("Find float = %v, want 0 because first map value is not guaranteed numeric", foundFloat)
	}

	// ToStruct/Find 对非指针输入当前会 panic，这里锁定现有行为。
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("Find non-pointer did not panic")
			}
		}()
		rows, err := db.Query("SELECT * FROM stub")
		if err != nil {
			t.Fatal(err)
		}
		_ = (&SQLRows{rows: rows}).Find(extraTaggedModel{})
	}()
	if err := (&SQLRows{err: driver.ErrBadConn}).ToStruct(&extraTaggedModel{}); err != nil {
		t.Fatalf("ToStruct with query error still returns nil for empty data, got %v", err)
	}
}

func TestExtraTableCreateOrUpdateReadAndDateBranches(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	crudMu.Lock()
	crudExecErr = errors.New("create failed")
	crudMu.Unlock()
	if err := table.CreateOrUpdate(map[string]any{"name": "bad"}); !errors.Is(err, ErrExec) {
		t.Fatalf("CreateOrUpdate create error = %v, want ErrExec", err)
	}

	db.tableColumns["empty"] = Columns{"id": {Name: "id"}, "name": {Name: "name"}}
	if row := db.Table("empty").Read(map[string]any{"id": 1}); !row.NotFound() {
		t.Fatalf("Read empty = %#v, want empty RowMap", row)
	}

	crudMu.Lock()
	crudExecErr = nil
	crudMu.Unlock()
	hardDelete := db.Table("hard_delete")
	db.tableColumns["hard_delete"] = Columns{"id": {Name: "id"}}
	if got, err := hardDelete.Delete(map[string]any{"id": 1}); err != nil || got != 2 {
		t.Fatalf("hard Delete = %d, %v; want 2, nil", got, err)
	}

	// Search 为空时 Clone 会补一个新的 Search，避免后续链式调用空指针。
	bare := &Table{DataBase: db, tableName: "user"}
	cloned := bare.Clone()
	if cloned == bare || cloned.Search == nil || cloned.Search.table != cloned || cloned.Search.tableName != "user" {
		t.Fatalf("Clone bare table = %#v", cloned)
	}

	base := newUnitTableWithDB("user", map[string][]string{
		"user": {"id", "created_at"},
	})
	// 只传结束时间的日期/月/时分过滤会按当前实现生成开始参数为空的条件。
	tests := []struct {
		name      string
		table     *Table
		wantQuery string
		wantArgs  []any
	}{
		{
			name:      "day end only",
			table:     base.WhereStartEndDay("created_at", "", "2026-05-14"),
			wantQuery: "SELECT * FROM `user` WHERE created_at >= ? AND created_at <= ?",
			wantArgs:  []any{" 00:00:00", "2026-05-14 23:59:59"},
		},
		{
			name:      "month end only",
			table:     base.WhereStartEndMonth("created_at", "", "2026-05"),
			wantQuery: "SELECT * FROM `user` WHERE DATE_FORMAT(created_at,'%Y-%m') >= ? AND DATE_FORMAT(created_at,'%Y-%m') <= ?",
			wantArgs:  []any{"", "2026-05"},
		},
		{
			name:      "time end only",
			table:     base.WhereStartEndTime("created_at", "", "09:30"),
			wantQuery: "SELECT * FROM `user` WHERE DATE_FORMAT(created_at,'%H:%i') >= ? AND DATE_FORMAT(created_at,'%H:%i') <= ?",
			wantArgs:  []any{"", "09:30"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParsed(t, tt.table, tt.wantQuery, tt.wantArgs)
		})
	}
}

func TestExtraStructToMapJSONFailureAndPeriodBranches(t *testing.T) {
	type badJSONStruct struct {
		ID      int
		Payload []func() `mx:"payload"`
	}
	table := &Table{Columns: Columns{
		"id":      {Name: "id"},
		"payload": {Name: "payload"},
	}}
	record := badJSONStruct{ID: 10, Payload: []func(){func() {}}}

	// 结构体字段 JSON 序列化失败时，structToMap 使用字符串 "null" 保持 SQL 参数可写。
	got := structToMap(reflect.ValueOf(record), table)
	if got["payload"] != "null" || got["id"] != 10 {
		t.Fatalf("structToMap bad json = %#v", got)
	}

	periodTests := []struct {
		st     string
		et     string
		wantSt string
		wantEt string
	}{
		{"2026", "", "2026-01-01 00:00:00", "2027-01-01 00:00:00"},
		{"2026-05-14 08", "", "2026-05-14 08:00:00", "2026-05-14 09:00:00"},
		{"2026-05-14 08:09:10", "", "2026-05-14 08:09:10", "2026-05-14 08:09:11"},
		{"2026-05-14", "2026-05-16", "2026-05-14 00:00:00", "2026-05-17 00:00:00"},
	}
	for _, tt := range periodTests {
		t.Run(tt.st+"/"+tt.et, func(t *testing.T) {
			gotSt, gotEt, err := periodParse(tt.st, tt.et)
			if err != nil {
				t.Fatalf("periodParse() error = %v", err)
			}
			if gotSt != tt.wantSt || gotEt != tt.wantEt {
				t.Fatalf("periodParse() = %q %q, want %q %q", gotSt, gotEt, tt.wantSt, tt.wantEt)
			}
		})
	}
	if _, _, err := periodParse("2026", "2026-05"); !errors.Is(err, errPeriodParse) {
		t.Fatalf("periodParse mismatched length error = %v", err)
	}

	var nullTime sql.NullTime
	if err := setReflectValue(reflect.ValueOf(&nullTime).Elem(), []byte("2026-05-14 08:09:10")); err != nil {
		t.Fatalf("setReflectValue NullTime error = %v", err)
	}
	if !nullTime.Valid || !nullTime.Time.Equal(time.Date(2026, 5, 14, 8, 9, 10, 0, time.Local)) {
		t.Fatalf("NullTime = %#v", nullTime)
	}
}

func TestExtraRowsMapMultiWarpEdges(t *testing.T) {
	if got := (RowsMap{}).MultiWarpByField("id"); len(got) != 0 {
		t.Fatalf("MultiWarpByField odd args = %#v, want empty", got)
	}
	rows := RowsMap{
		{"province_id": "1", "province": "A", "city_id": "0", "city": ""},
		{"province_id": "1", "province": "A", "city_id": "2", "city": "B"},
	}
	got := rows.MultiWarpByField("province_id", "province", "city_id", "city")
	if len(got) != 1 || got[0].ID != "1" || len(got[0].Vals) != 1 || got[0].Vals[0].ID != "2" {
		t.Fatalf("MultiWarpByField zero child = %#v", got)
	}
	if row := rows.RowID("missing"); row != nil {
		t.Fatalf("RowID missing = %#v, want nil", row)
	}
}

func TestExtraNewDataBaseRegisteredPrefixDrivers(t *testing.T) {
	tests := []struct {
		dsn    string
		driver string
	}{
		{"postgres://ok", "postgres"},
		{"dm://ok", "dm"},
		{"db2://ok", "db2"},
		{"sqlserver://ok", "sqlserver"},
	}
	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			// 这些分支只验证 DSN 前缀到 driver 名称的选择，不依赖真实数据库服务。
			db, err := NewDataBase(tt.dsn, Config{Timeout: time.Second})
			if err != nil {
				t.Fatalf("NewDataBase(%q) error = %v", tt.dsn, err)
			}
			defer db.DB().Close()
			if db.Driver != tt.driver || db.tableColumns == nil || db.mm == nil {
				t.Fatalf("NewDataBase(%q) = driver %q tableColumns %#v mm %#v", tt.dsn, db.Driver, db.tableColumns, db.mm)
			}
		})
	}
}

type extraLogger struct {
	logs []LogSqlData
	errs []LogSqlData
}

func (l *extraLogger) LogSql(data LogSqlData) {
	l.logs = append(l.logs, data)
}

func (l *extraLogger) ErrSql(data LogSqlData) {
	l.errs = append(l.errs, data)
}
