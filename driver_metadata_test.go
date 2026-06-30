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

const (
	dmRowsDriverName   = "mx_dm_rows_stub_test"
	slowPingDriverName = "mx_slow_ping_stub_test"
)

func init() {
	sql.Register(dmRowsDriverName, dmRowsDriver{})
	sql.Register(slowPingDriverName, slowPingDriver{})
	sql.Register("postgres", prefixPingDriver{})
	sql.Register("dm", prefixPingDriver{})
	sql.Register("go_ibm_db", prefixPingDriver{})
	sql.Register("sqlserver", prefixPingDriver{})
}

type dmRowsDriver struct{}

func (dmRowsDriver) Open(name string) (driver.Conn, error) {
	return dmRowsConn{}, nil
}

type dmRowsConn struct{}

func (dmRowsConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (dmRowsConn) Close() error { return nil }

func (dmRowsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (dmRowsConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return &dmRowsStub{
		cols:  []string{"ID", "CREATED_AT", "BIRTHDAY", "NAME", "EMPTY_AT"},
		types: []string{"NUMBER", "TIMESTAMP", "DATE", "VARCHAR", "TIMESTAMP"},
		rows: [][]driver.Value{
			{
				[]byte("7"),
				time.Date(2026, 5, 14, 8, 9, 10, 0, time.Local),
				time.Date(2026, 5, 14, 0, 0, 0, 0, time.Local),
				[]byte("alice"),
				nil,
			},
		},
	}, nil
}

type dmRowsStub struct {
	cols  []string
	types []string
	rows  [][]driver.Value
	idx   int
}

func (r *dmRowsStub) Columns() []string { return r.cols }
func (r *dmRowsStub) Close() error      { return nil }

func (r *dmRowsStub) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func (r *dmRowsStub) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}

type slowPingDriver struct{}

func (slowPingDriver) Open(name string) (driver.Conn, error) {
	return slowPingConn{}, nil
}

type slowPingConn struct{}

func (slowPingConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (slowPingConn) Close() error { return nil }

func (slowPingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (slowPingConn) Ping(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type prefixPingDriver struct{}

func (prefixPingDriver) Open(name string) (driver.Conn, error) {
	return prefixPingConn{name: name}, nil
}

type prefixPingConn struct {
	name string
}

func (prefixPingConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (prefixPingConn) Close() error { return nil }

func (prefixPingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (c prefixPingConn) Ping(ctx context.Context) error {
	if c.name == "postgres://unit" || c.name == "dm://unit" || c.name == "unit" {
		return errors.New("prefix ping failed")
	}
	return nil
}

type CoverageTarget struct {
	ID int
}

type CoverageBelong struct {
	ID               int
	CoverageTargetID int
}

type CoverageOwner struct {
	ID int
}

type CoverageChild struct {
	ID   int
	Name string
}

type CoverageParent struct {
	ID               int
	Name             string
	CoverageChild    CoverageChild
	CoverageChildren []CoverageChild
}

type coverageEmbeddedFields struct {
	EmbeddedName string `json:"embedded_name"`
}

type coverageRichModel struct {
	coverageEmbeddedFields
	ID               int    `json:"id"`
	Alias            string `mx:"alias"`
	Named            string `json:"named,omitempty"`
	Ignored          string `mx:"-"`
	CoverageChild    CoverageChild
	CoverageChildren []CoverageChild
	Calls            int `mx:"-"`
}

func (m *coverageRichModel) AfterFind() error {
	m.Calls++
	return nil
}

func TestDMRowsMapCoversDateTimeAndNull(t *testing.T) {
	db, err := sql.Open(dmRowsDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT dm rows")
	if err != nil {
		t.Fatal(err)
	}

	// 达梦适配会把列名转成小写，并按数据库类型格式化日期时间。
	got := (&SQLRows{driver: "dm", rows: rows}).RowsMap()
	want := RowsMap{{
		"id":         "7",
		"created_at": "2026-05-14 08:09:10",
		"birthday":   "2026-05-14",
		"name":       "alice",
		"empty_at":   "",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dm RowsMap() = %#v, want %#v", got, want)
	}

	// 错误和空 rows 都应该安全返回空切片，不能 panic。
	if got := (&SQLRows{driver: "dm", err: errors.New("query failed")}).RowsMap(); len(got) != 0 {
		t.Fatalf("dm RowsMap error = %#v, want empty", got)
	}
	if got := (&SQLRows{driver: "dm"}).RowsMap(); len(got) != 0 {
		t.Fatalf("dm RowsMap nil rows = %#v, want empty", got)
	}
}

func TestConversionHelpersCoverPrimitiveBranches(t *testing.T) {
	// String 的特殊分支用于避免常见基础类型走 fmt 的慢路径。
	stringCases := []struct {
		name string
		in   any
		want string
	}{
		{"int64", int64(42), "42"},
		{"bytes", []byte("abc"), "abc"},
		{"byte", byte('A'), "A"},
		{"default", 2.5, "2.5"},
	}
	for _, tt := range stringCases {
		t.Run("String "+tt.name, func(t *testing.T) {
			if got := String(tt.in); got != tt.want {
				t.Fatalf("String(%#v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}

	if got := Float("2.25"); got != 2.25 {
		t.Fatalf("Float() = %v, want 2.25", got)
	}

	// Int 支持多种整数类型和字节切片，这里把每个类型分支都跑一遍。
	intCases := []struct {
		name string
		in   any
		want int
	}{
		{"string", "11", 11},
		{"int", int(12), 12},
		{"int8", int8(13), 13},
		{"int16", int16(14), 14},
		{"int32", int32(15), 15},
		{"int64", int64(16), 16},
		{"uint", uint(17), 17},
		{"uint8", uint8(18), 18},
		{"uint16", uint16(19), 19},
		{"uint32", uint32(20), 20},
		{"uint64", uint64(21), 21},
		{"bytes", []byte("22"), 22},
		{"negative bytes", []byte("-23"), -23},
		{"empty bytes", []byte{}, 0},
		{"default", 24.0, 24},
	}
	for _, tt := range intCases {
		t.Run("Int "+tt.name, func(t *testing.T) {
			if got := Int(tt.in); got != tt.want {
				t.Fatalf("Int(%#v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestSetReflectValueBlankJSONAndNilTargets(t *testing.T) {
	var m map[string]int

	// 空白 JSON 输入会被当成零值，避免把空字符串传给 json.Unmarshal。
	if err := setReflectValue(reflect.ValueOf(&m).Elem(), []byte("   ")); err != nil {
		t.Fatalf("setReflectValue blank map error = %v", err)
	}
	if m != nil {
		t.Fatalf("blank map = %#v, want nil zero value", m)
	}

	// 不可设置或无效的 reflect.Value 应直接忽略，方便上层扫描容错。
	if err := setReflectValue(reflect.Value{}, []byte("1")); err != nil {
		t.Fatalf("setReflectValue invalid value error = %v", err)
	}
	value := 1
	if err := setReflectValue(reflect.ValueOf(value), []byte("2")); err != nil {
		t.Fatalf("setReflectValue unsettable value error = %v", err)
	}
	if value != 1 {
		t.Fatalf("unsettable value changed to %d", value)
	}

	var runes []rune
	// setReflectBytes 只接受 []byte 或可转换的别名，其他切片类型要返回错误。
	if err := setReflectBytes(reflect.ValueOf(&runes).Elem(), []byte("abc")); err == nil {
		t.Fatalf("setReflectBytes []rune error = nil")
	}
}

func TestSearchAdditionalBranches(t *testing.T) {
	table := newUnitTableWithDB("user", map[string][]string{
		"user": {"id", "name"},
	})

	// 空字段列表和空 NOT IN 条件应保持原查询不变。
	if got := table.Fields(); got != table {
		t.Fatalf("Fields() empty returned different table")
	}
	assertParsed(t, table.NotIn("id").NotIn("id", []int{}), "SELECT * FROM `user`", nil)
	assertParsed(t, table.WhereLikeLeft("name", ""), "SELECT * FROM `user`", nil)
	assertParsed(t, table.WhereLikeRight("name", ""), "SELECT * FROM `user`", nil)

	// NOT IN 传入切片时需要展开成多个占位符。
	assertParsed(t, table.NotIn("id", []any{1, 2}), "SELECT * FROM `user` WHERE id NOT IN (?,?)", []any{1, 2})

	// raw 查询模式直接复用已有 SQL 和参数，不再走拼装逻辑。
	raw := table.Search.Clone()
	raw.raw = true
	raw.query = "SELECT raw WHERE id = ?"
	raw.args = []any{7}
	gotQuery, gotArgs := raw.Parse()
	if gotQuery != raw.query || !reflect.DeepEqual(gotArgs, raw.args) {
		t.Fatalf("raw Parse() = %q %#v, want %q %#v", gotQuery, gotArgs, raw.query, raw.args)
	}

	// MustIn 空集合会短路，所有读取方法都应返回空值且不触发查询。
	noQuery := table.MustIn("id")
	if row := noQuery.RowMap(); len(row) != 0 {
		t.Fatalf("noNeed RowMap = %#v, want empty", row)
	}
	if row := noQuery.RowMapInterface(); len(row) != 0 {
		t.Fatalf("noNeed RowMapInterface = %#v, want empty", row)
	}
	if rows := noQuery.RowsMapInterface(); len(rows) != 0 {
		t.Fatalf("noNeed RowsMapInterface = %#v, want empty", rows)
	}
	if rows := noQuery.RowsMapNull(); len(rows) != 0 {
		t.Fatalf("noNeed RowsMapNull = %#v, want empty", rows)
	}
	if cols, rows := noQuery.DoubleSlice(); len(cols) != 0 || len(rows) != 0 {
		t.Fatalf("noNeed DoubleSlice = %#v %#v, want empty", cols, rows)
	}
	if got := noQuery.Int(); got != 0 {
		t.Fatalf("noNeed Int = %d, want 0", got)
	}
	if got := noQuery.String(); got != "" {
		t.Fatalf("noNeed String = %q, want empty", got)
	}
	if got := noQuery.Float(); got != 0 {
		t.Fatalf("noNeed Float = %v, want 0", got)
	}
}

func TestTableBatchDebugCountAndEachAddTableString(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	// 批量 IN 会按 batchSize 拆多次查询，stub 每次固定返回两行。
	if got := table.InBatch(1, "id", 1, 2); len(got) != 4 {
		t.Fatalf("InBatch() len = %d, want 4", len(got))
	}
	if got := table.InAuto("id", 1, 2); len(got) != 2 {
		t.Fatalf("InAuto() len = %d, want 2", len(got))
	}

	// 当天和早于当天的条件包含当前日期，断言 SQL 形状和日期参数即可。
	todayQuery, todayArgs := table.WhereToday("created_at").Parse()
	wantToday := time.Now().Format("2006-01-02")
	if !strings.Contains(todayQuery, "created_at >=") || !strings.Contains(todayQuery, "created_at <") || len(todayArgs) != 1 {
		t.Fatalf("WhereToday query=%q args=%#v", todayQuery, todayArgs)
	}
	beforeQuery, beforeArgs := table.WhereBeforeToday("created_at").Parse()
	if beforeQuery != "SELECT * FROM `user` WHERE DATE_FORMAT(created_at,'%Y-%m-%d') < ? AND user.is_deleted = ?" ||
		!reflect.DeepEqual(beforeArgs, []any{wantToday, 0}) {
		t.Fatalf("WhereBeforeToday query=%q args=%#v", beforeQuery, beforeArgs)
	}

	crudMu.Lock()
	crudCountValue = "5"
	crudMu.Unlock()
	if got := table.Where("age > ?", 18).Count(); got != 5 {
		t.Fatalf("Count() = %d, want 5", got)
	}

	// 表级 Debug 只切换 Search 标记，真实查询仍委托给 DataBase。
	if table.Debug() != table || !table.Search.debug {
		t.Fatalf("Debug() did not enable table search debug")
	}
	if got := table.Query("SELECT * FROM user WHERE id=?", 1).RowsMap(); len(got) != 2 {
		t.Fatalf("debug Query rows = %#v", got)
	}
	if _, err := table.Exec("UPDATE user SET name=?", "debug").RowsAffected(); err != nil {
		t.Fatalf("debug Exec error = %v", err)
	}
	db.Debug(true)
	if got := table.Query("SELECT * FROM user").RowsMap(); len(got) != 2 {
		t.Fatalf("db debug Query rows = %#v", got)
	}
	db.Debug(false)
	if table.DebugClose() != table || table.Search.debug {
		t.Fatalf("DebugClose() did not disable table search debug")
	}

	called := false
	if got := table.QueryFunc(func(qt *Table) { called = qt == table }); got != table || !called {
		t.Fatalf("QueryFunc() returned %#v called=%v", got, called)
	}

	if explain := table.WhereID(1).Explain(true); explain.ID != 1 {
		t.Fatalf("Explain(true) = %#v", explain)
	}

	// EachAddTableString 用另一张表的查询结果回填当前 RowsMap。
	rows := RowsMap{{"uid": "1"}, {"uid": "9"}}
	rows.EachAddTableString(table, "uid", "id", "name", "user_name")
	if rows[0]["user_name"] != "alice" || rows[1]["user_name"] != "" {
		t.Fatalf("EachAddTableString rows = %#v", rows)
	}
	before := rows.Copy()
	rows.EachAddTableString(table, "uid", "id", "name")
	if !reflect.DeepEqual(rows, before) {
		t.Fatalf("EachAddTableString invalid args changed rows: %#v -> %#v", before, rows)
	}
}

func TestResultAndSortAdditionalBranches(t *testing.T) {
	errRows := &SQLRows{err: errors.New("scan failed")}

	// SQLRows 出错时，便捷读取方法统一返回零值，方便调用方链式处理。
	if got := errRows.PluckInt("id"); len(got) != 0 {
		t.Fatalf("PluckInt error = %#v, want empty", got)
	}
	if got := errRows.PluckString("id"); len(got) != 0 {
		t.Fatalf("PluckString error = %#v, want empty", got)
	}
	if got := errRows.RowMap(); len(got) != 0 {
		t.Fatalf("RowMap error = %#v, want empty", got)
	}
	if got := errRows.Int(); got != 0 {
		t.Fatalf("Int error = %d, want 0", got)
	}
	if got := errRows.String(); got != "" {
		t.Fatalf("String error = %q, want empty", got)
	}
	if cols, rows := errRows.DoubleSlice(); len(cols) != 0 || len(rows) != 0 {
		t.Fatalf("DoubleSlice error = %#v %#v, want empty", cols, rows)
	}
	if cols, rows := errRows.TripleByte(); len(cols) != 0 || len(rows) != 0 {
		t.Fatalf("TripleByte error = %#v %#v, want empty", cols, rows)
	}

	var f float64
	errRows.setValue(reflect.ValueOf(&f).Elem(), "3.5")
	if f != 3.5 {
		t.Fatalf("setValue float = %v, want 3.5", f)
	}
	var b bool
	errRows.setValue(reflect.ValueOf(&b).Elem(), true)
	if !b {
		t.Fatalf("setValue default bool = false, want true")
	}

	rowsMap := RowsMap{
		{"id": "1", "name": "a", "amount": "1.5"},
		{"id": "2", "name": "c", "amount": "3.5"},
		{"id": "3", "name": "b", "amount": "2.5"},
	}
	rowsMap.Sort("name", true)
	if got := rowsMap.PluckString("name"); !reflect.DeepEqual(got, []string{"c", "b", "a"}) {
		t.Fatalf("Sort desc = %#v", got)
	}
	rowsMap.SortFloat("amount", true)
	if got := rowsMap.PluckString("amount"); !reflect.DeepEqual(got, []string{"3.5", "2.5", "1.5"}) {
		t.Fatalf("SortFloat desc = %#v", got)
	}
}

func TestTableFullMemberCreatesAndDeletesMissingMembers(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	// FullMember 会删除旧集合里不存在的成员，并创建新集合里缺失的成员。
	err := table.FullMember(
		[]map[string]string{
			{"name": "alice"},
			{"name": "carol"},
		},
		"group_id",
		"g1",
		"name",
	)
	if err != nil {
		t.Fatalf("FullMember() error = %v", err)
	}

	crudMu.Lock()
	execs := append([]string(nil), crudExecs...)
	crudMu.Unlock()

	var sawDelete, sawCreate bool
	for _, query := range execs {
		if strings.Contains(query, "UPDATE `user` SET is_deleted") {
			sawDelete = true
		}
		if strings.Contains(query, "INSERT INTO `user`") {
			sawCreate = true
		}
	}
	if !sawDelete || !sawCreate {
		t.Fatalf("FullMember execs = %#v, want delete and create", execs)
	}
}

func TestDataBaseFindConnectionAndNestedFindAllWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	db.tableColumns["coverage_parent"] = Columns{
		"id":   {Name: "id"},
		"name": {Name: "name"},
	}
	db.tableColumns["coverage_child"] = Columns{
		"id":                 {Name: "id"},
		"name":               {Name: "name"},
		"coverage_parent_id": {Name: "coverage_parent_id"},
	}

	if err := db.Find(CoverageParent{}, "SELECT * FROM coverage_parent"); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("Find non-pointer error = %v, want ErrMustBeAddr", err)
	}
	// Find 覆盖原生 SQL、ID 条件和自定义 where 条件。
	var rawOne CoverageParent
	if err := db.Find(&rawOne, "SELECT * FROM coverage_parent WHERE id=?", 1); err != nil {
		t.Fatalf("Find raw SQL error = %v", err)
	}
	if rawOne.ID != 1 || rawOne.Name != "alice" {
		t.Fatalf("Find raw SQL = %#v", rawOne)
	}
	var byID CoverageParent
	if err := db.Find(&byID, 1); err != nil {
		t.Fatalf("Find by id error = %v", err)
	}
	if byID.ID != 1 || byID.Name != "alice" {
		t.Fatalf("Find by id = %#v", byID)
	}
	var many []CoverageParent
	if err := db.Find(&many, "age > ?", 18); err != nil {
		t.Fatalf("Find where slice error = %v", err)
	}
	if len(many) != 2 || many[0].Name != "alice" {
		t.Fatalf("Find where slice = %#v", many)
	}

	// FindAll 会继续填充结构体和切片类型的关联字段。
	var parent CoverageParent
	if err := db.FindAll(&parent, "SELECT * FROM coverage_parent WHERE id=?", 1); err != nil {
		t.Fatalf("FindAll parent error = %v", err)
	}
	if parent.CoverageChild.Name != "alice" || len(parent.CoverageChildren) != 2 {
		t.Fatalf("FindAll parent = %#v", parent)
	}
	var parentRows []CoverageParent
	if err := db.FindAll(&parentRows, "SELECT * FROM coverage_parent"); err != nil {
		t.Fatalf("FindAll slice error = %v", err)
	}
	if len(parentRows) != 2 || len(parentRows[0].CoverageChildren) != 2 {
		t.Fatalf("FindAll slice = %#v", parentRows)
	}
	if err := db.FindAll(new(int), "SELECT 1"); !errors.Is(err, ErrNotSupportType) {
		t.Fatalf("FindAll unsupported error = %v, want ErrNotSupportType", err)
	}
}

func TestModelStructRichMappingAndGuessRelations(t *testing.T) {
	db := resetCRUDStubDB(t)
	db.tableColumns["coverage_parent"] = Columns{
		"id":            {Name: "id"},
		"embedded_name": {Name: "embedded_name"},
		"alias":         {Name: "alias"},
		"named":         {Name: "named"},
	}
	db.tableColumns["coverage_child"] = Columns{
		"id":                 {Name: "id"},
		"name":               {Name: "name"},
		"coverage_parent_id": {Name: "coverage_parent_id"},
	}
	db.tableColumns["coverage_children"] = Columns{
		"id":                 {Name: "id"},
		"name":               {Name: "name"},
		"coverage_parent_id": {Name: "coverage_parent_id"},
	}
	table := db.Table("coverage_parent")
	cols := map[string]int{"id": 0, "embedded_name": 1, "alias": 2, "named": 3}
	row := [][]byte{[]byte("7"), []byte("embed"), []byte("alias value"), []byte("json name")}

	// SetStruct 同时覆盖匿名结构体、mx 标签、json 标签和自动猜测关联。
	var model coverageRichModel
	ms, err := NewModelStruct(&model)
	if err != nil {
		t.Fatalf("NewModelStruct rich model error = %v", err)
	}
	if err := ms.SetStruct(table, cols, row, true); err != nil {
		t.Fatalf("SetStruct rich model error = %v", err)
	}
	if model.ID != 7 || model.EmbeddedName != "embed" || model.Alias != "alias value" || model.Named != "json name" {
		t.Fatalf("SetStruct scalar fields = %#v", model)
	}
	if model.CoverageChild.Name != "alice" || len(model.CoverageChildren) != 2 || model.Calls != 1 {
		t.Fatalf("SetStruct guessed relations = %#v", model)
	}

	// ModelStruct.setSlice 应对每一行执行同样的字段映射和 AfterFind 回调。
	var models []coverageRichModel
	ms, err = NewModelStruct(&models)
	if err != nil {
		t.Fatalf("NewModelStruct rich slice error = %v", err)
	}
	datas := [][][]byte{
		row,
		{[]byte("8"), []byte("embed2"), []byte("alias2"), []byte("json2")},
	}
	if err := ms.setSlice(table, cols, datas, true); err != nil {
		t.Fatalf("setSlice rich models error = %v", err)
	}
	if len(models) != 2 || models[1].ID != 8 || models[1].EmbeddedName != "embed2" || models[1].Calls != 1 {
		t.Fatalf("setSlice rich models = %#v", models)
	}
	if models[0].CoverageChild.Name != "alice" || len(models[0].CoverageChildren) != 2 {
		t.Fatalf("setSlice guessed relations = %#v", models[0])
	}
}

func TestDataBaseConnectionBranches(t *testing.T) {
	db := newUnitDB(map[string][]string{
		"coverage_target":               {"id"},
		"coverage_belong":               {"id", "coverage_target_id"},
		"coverage_owner":                {"id"},
		"coverage_item":                 {"id", "coverage_owner_id"},
		"coverage_label":                {"id"},
		"coverage_label_coverage_owner": {"coverage_label_id", "coverage_owner_id"},
	})

	// got 表包含 target_id 时，走“已知从表找主表”的分支。
	con, ok := db.connection("coverage_target", reflect.ValueOf(CoverageBelong{ID: 4, CoverageTargetID: 9}))
	if !ok || len(con) != 2 || con[1] != 9 || !strings.Contains(con[0].(string), "SELECT `coverage_belong`.*") {
		t.Fatalf("connection belong = %#v, %v", con, ok)
	}

	// target 表包含 got_id 时，走“一对多”分支。
	con, ok = db.connection("coverage_item", reflect.ValueOf(CoverageOwner{ID: 6}))
	if !ok || len(con) != 2 || con[1] != 6 || con[0] != "SELECT * FROM `coverage_item` WHERE coverage_owner_id = ?" {
		t.Fatalf("connection has-many = %#v, %v", con, ok)
	}

	// 中间表同时包含两侧外键时，走多对多 join 分支。
	con, ok = db.connection("coverage_label", reflect.ValueOf(CoverageOwner{ID: 6}))
	if !ok || len(con) != 2 || con[1] != 6 || !strings.Contains(con[0].(string), "LEFT JOIN coverage_label_coverage_owner") {
		t.Fatalf("connection many-to-many = %#v, %v", con, ok)
	}

	con, ok = db.connection("missing_table", reflect.ValueOf(CoverageOwner{ID: 6}))
	if ok || len(con) != 0 {
		t.Fatalf("connection missing = %#v, %v; want no match", con, ok)
	}
}

func TestDatabaseMetadataAndInitErrorBranches(t *testing.T) {
	raw, err := sql.Open(miniDBDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// Config.parse 当前没有副作用，调用一次用于锁定无操作行为。
	conf := Config{Timeout: time.Second, MaxIdleConns: 1, MaxOpenConns: 2}
	conf.parse()
	if conf.Timeout != time.Second || conf.MaxIdleConns != 1 || conf.MaxOpenConns != 2 || conf.IgnoreNoDatabaseWarning {
		t.Fatalf("Config.parse changed config: %#v", conf)
	}

	// 未命中缓存时，getColumns 会查询 information_schema 并写回缓存。
	db := &DataBase{db: raw, tableColumns: map[string]Columns{}, mm: new(sync.RWMutex)}
	cols := db.getColumns("user")
	if !cols.HaveColumn("id") || !cols.HaveColumn("name") {
		t.Fatalf("getColumns uncached = %#v", cols)
	}
	if cached := db.tableColumns["user"]; !cached.HaveColumn("id") {
		t.Fatalf("getColumns did not cache result: %#v", db.tableColumns)
	}

	// 当前仓库没有注册这些驱动，NewDataBase 应在 sql.Open 阶段返回错误。
	for _, dsn := range []string{"postgres://unit", "dm://unit", "db2://unit", "sqlserver://unit"} {
		t.Run(dsn, func(t *testing.T) {
			if got, err := NewDataBase(dsn, Config{Timeout: time.Millisecond}); err == nil {
				if got != nil && got.DB() != nil {
					_ = got.DB().Close()
				}
				t.Fatalf("NewDataBase(%q) error = nil", dsn)
			}
		})
	}
	if got, err := NewDataBase("bad mysql dsn", Config{Timeout: time.Millisecond}); err == nil {
		if got != nil && got.DB() != nil {
			_ = got.DB().Close()
		}
		t.Fatalf("NewDataBase bad mysql dsn error = nil")
	}

	// PingContext 超时要转成连接初始化错误，而不是一直阻塞。
	slow, err := sql.Open(slowPingDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if err := checkDBInitContext(slow, Config{Timeout: time.Millisecond}); err == nil {
		t.Fatalf("checkDBInitContext slow ping error = nil")
	}
}
