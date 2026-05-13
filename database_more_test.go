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
)

const crudDBDriverName = "mx_crud_stub_test"

var (
	crudMu         sync.Mutex
	crudQueries    []string
	crudExecs      []string
	crudCountValue = "0"
	crudExecErr    error
)

func init() {
	sql.Register(crudDBDriverName, crudDriver{})
}

type crudDriver struct{}

func (crudDriver) Open(name string) (driver.Conn, error) {
	return crudConn{}, nil
}

type crudConn struct{}

func (crudConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (crudConn) Close() error { return nil }

func (crudConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (crudConn) Ping(ctx context.Context) error { return nil }

func (crudConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	crudMu.Lock()
	crudQueries = append(crudQueries, query)
	count := crudCountValue
	crudMu.Unlock()

	lower := strings.ToLower(query)
	switch {
	case strings.HasPrefix(lower, "explain "):
		return crudRowsFor(
			[]string{"id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "extra"},
			[]driver.Value{[]byte("1"), []byte("SIMPLE"), []byte("user"), nil, []byte("const"), []byte("PRIMARY"), []byte("PRIMARY"), []byte("4"), []byte("const"), []byte("1"), []byte("100"), []byte("Using index")},
		), nil
	case strings.HasPrefix(lower, "show index"):
		return crudRowsFromValues(
			[]string{"table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"},
			[][]driver.Value{
				{[]byte("user"), []byte("0"), []byte("PRIMARY"), []byte("1"), []byte("id"), []byte("A"), []byte("2"), nil, nil, []byte(""), []byte("BTREE"), []byte(""), []byte(""), []byte("YES"), nil},
				{[]byte("user"), []byte("1"), []byte("idx_name"), []byte("2"), []byte("age"), []byte("A"), []byte("2"), nil, nil, []byte("YES"), []byte("BTREE"), []byte(""), []byte(""), []byte("YES"), nil},
				{[]byte("user"), []byte("1"), []byte("idx_name"), []byte("1"), []byte("name"), []byte("A"), []byte("2"), nil, nil, []byte("YES"), []byte("BTREE"), []byte(""), []byte(""), []byte("YES"), nil},
			},
		), nil
	case strings.Contains(lower, "update_time"):
		return crudRowsFor([]string{"UPDATE_TIME"}, []driver.Value{[]byte("2026-05-13 12:00:00")}), nil
	case strings.Contains(lower, "auto_increment"):
		return crudRowsFor([]string{"AUTO_INCREMENT"}, []driver.Value{[]byte("12")}), nil
	case strings.Contains(lower, "ifnull(max(id)"):
		return crudRowsFor([]string{"id"}, []driver.Value{[]byte("99")}), nil
	case strings.Contains(lower, "count(*)"):
		return crudRowsFor([]string{"total"}, []driver.Value{[]byte(count)}), nil
	default:
		return crudUserRows(), nil
	}
}

func (crudConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	crudMu.Lock()
	defer crudMu.Unlock()
	crudExecs = append(crudExecs, query)
	if crudExecErr != nil {
		return nil, crudExecErr
	}
	return sqlResultStub{id: 17, rows: 2}, nil
}

type crudRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func crudRowsFor(cols []string, row []driver.Value) *crudRows {
	return crudRowsFromValues(cols, [][]driver.Value{row})
}

func crudRowsFromValues(cols []string, rows [][]driver.Value) *crudRows {
	return &crudRows{cols: cols, rows: rows}
}

func crudUserRows() *crudRows {
	return crudRowsFromValues(
		[]string{"id", "name", "age", "active", "amount"},
		[][]driver.Value{
			{[]byte("1"), []byte("alice"), []byte("30"), []byte("1"), []byte("2.5")},
			{[]byte("2"), []byte("bob"), []byte("40"), []byte("0"), []byte("3.5")},
		},
	)
}

func (r *crudRows) Columns() []string { return r.cols }
func (r *crudRows) Close() error      { return nil }
func (r *crudRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func resetCRUDStubDB(t *testing.T) *DataBase {
	t.Helper()
	crudMu.Lock()
	crudQueries = nil
	crudExecs = nil
	crudCountValue = "0"
	crudExecErr = nil
	crudMu.Unlock()

	raw, err := sql.Open(crudDBDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		raw.Close()
		crudMu.Lock()
		crudExecErr = nil
		crudCountValue = "0"
		crudMu.Unlock()
	})

	userCols := Columns{
		"id":         {Name: "id"},
		"name":       {Name: "name"},
		"age":        {Name: "age"},
		"active":     {Name: "active"},
		"amount":     {Name: "amount"},
		"created_at": {Name: "created_at", DataType: "datetime"},
		IsDeleted:    {Name: IsDeleted},
		"deleted_at": {Name: "deleted_at", DataType: "datetime"},
	}
	return &DataBase{
		Schema: "unit_schema",
		db:     raw,
		tableColumns: map[string]Columns{
			"user": userCols,
		},
		mm: new(sync.Mutex),
	}
}

type crudSaveModel struct {
	ID                int    `mx:"id"`
	Name              string `mx:"name"`
	Age               int    `mx:"age"`
	CreatedAt         string `mx:"created_at"`
	BeforeCreateCount int    `mx:"-"`
	AfterCreateCount  int    `mx:"-"`
	BeforeUpdateCount int    `mx:"-"`
	AfterUpdateCount  int    `mx:"-"`
}

func (crudSaveModel) DBName() string { return "user" }

func (m *crudSaveModel) BeforeCreate() error {
	m.BeforeCreateCount++
	return nil
}

func (m *crudSaveModel) AfterCreate() error {
	m.AfterCreateCount++
	return nil
}

func (m *crudSaveModel) BeforeUpdate() error {
	m.BeforeUpdateCount++
	return nil
}

func (m *crudSaveModel) AfterUpdate() error {
	m.AfterUpdateCount++
	return nil
}

func TestDataBaseQueryExecAndTableMetadataWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	if got := table.UpdateTime(); got != "2026-05-13 12:00:00" {
		t.Fatalf("UpdateTime() = %q", got)
	}
	if got := table.AutoIncrement(); got != 12 {
		t.Fatalf("AutoIncrement() = %d, want 12", got)
	}
	if got := table.MaxID(); got != 99 {
		t.Fatalf("MaxID() = %d, want 99", got)
	}
	if err := table.SetAutoIncrement(50); err != nil {
		t.Fatalf("SetAutoIncrement() error = %v", err)
	}

	if rows := table.IDIn(); len(rows.RowsMap()) != 0 {
		t.Fatalf("IDIn empty = %#v, want empty", rows.RowsMap())
	}
	if rows := table.IDIn(1, 2).RowsMap(); len(rows) != 2 {
		t.Fatalf("IDIn rows = %#v", rows)
	}
	if row := table.IDRow(1); row["name"] != "alice" {
		t.Fatalf("IDRow(1) = %#v", row)
	}

	indexes := table.Indexs()
	if indexes["idx_name"][0].ColumnName != "name" || indexes["idx_name"][1].ColumnName != "age" {
		t.Fatalf("Indexs idx_name not sorted by Seq_in_index: %#v", indexes["idx_name"])
	}
	if again := table.Indexs(); !reflect.DeepEqual(again, indexes) {
		t.Fatalf("Indexs cache changed: %#v vs %#v", again, indexes)
	}

	gotRows := db.Query("SELECT * FROM user").RowsMap()
	if len(gotRows) != 2 || gotRows[0]["name"] != "alice" {
		t.Fatalf("DataBase.Query rows = %#v", gotRows)
	}
	affected, err := db.Exec("UPDATE user SET name=?", "alice").RowsAffected()
	if err != nil || affected != 2 {
		t.Fatalf("DataBase.Exec RowsAffected = %d, %v", affected, err)
	}
}

func TestTableCRUDHelpersWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	id, err := table.Create(map[string]any{"name": "alice", "age": 30}, "name")
	if err != nil || id != 17 {
		t.Fatalf("Create() = %d, %v; want 17, nil", id, err)
	}

	crudMu.Lock()
	crudCountValue = "1"
	crudMu.Unlock()
	if _, err := table.Create(map[string]any{"name": "alice"}, "name"); !errors.Is(err, ErrInsertRepeat) {
		t.Fatalf("Create repeat error = %v, want ErrInsertRepeat", err)
	}

	if got, err := table.Creates(nil); err != nil || got != 0 {
		t.Fatalf("Creates nil = %d, %v; want 0, nil", got, err)
	}
	crudMu.Lock()
	crudCountValue = "0"
	crudMu.Unlock()
	if got, err := table.Creates([]map[string]any{{"name": "a"}, {"name": "b"}}); err != nil || got != 2 {
		t.Fatalf("Creates two = %d, %v; want 2, nil", got, err)
	}

	if got, err := table.DeleteID(1); err != nil || got != 2 {
		t.Fatalf("DeleteID() = %d, %v; want 2, nil", got, err)
	}
	if got, err := table.DeleteIDs(); err != nil || got != 0 {
		t.Fatalf("DeleteIDs empty = %d, %v; want 0, nil", got, err)
	}
	if got, err := table.DeleteIDs([]int{}); err != nil || got != 0 {
		t.Fatalf("DeleteIDs empty slice = %d, %v; want 0, nil", got, err)
	}
	if got, err := table.DeleteIDs([]int{1, 2}); err != nil || got != 2 {
		t.Fatalf("DeleteIDs slice = %d, %v; want 2, nil", got, err)
	}
	if got, err := table.Delete(map[string]any{}); !errors.Is(err, ErrNoDeleteKey) || got != 0 {
		t.Fatalf("Delete empty = %d, %v; want ErrNoDeleteKey", got, err)
	}
	if got, err := table.Delete(map[string]any{"id": 1}); err != nil || got != 2 {
		t.Fatalf("Delete id = %d, %v; want 2, nil", got, err)
	}

	if got, err := table.Update(map[string]any{"name": "no id"}); !errors.Is(err, ErrNoUpdateKey) || got != 0 {
		t.Fatalf("Update missing id = %d, %v; want ErrNoUpdateKey", got, err)
	}
	if got, err := table.Update(map[string]any{"id;drop": 1, "name": "x"}, "id;drop"); !errors.Is(err, ErrMayBeAttack) || got != 0 {
		t.Fatalf("Update attack key = %d, %v; want ErrMayBeAttack", got, err)
	}
	updateMap := map[string]any{"id": 1, "name": "new"}
	if got, err := table.Update(updateMap); err != nil || got != 2 || updateMap["id"] != 1 {
		t.Fatalf("Update success = %d, %v, map %#v", got, err, updateMap)
	}

	crudMu.Lock()
	crudExecErr = errors.New("exec failed")
	crudMu.Unlock()
	if _, err := table.Create(map[string]any{"name": "bad"}); !errors.Is(err, ErrExec) {
		t.Fatalf("Create exec error = %v, want ErrExec", err)
	}
	if _, err := table.Update(map[string]any{"id": 1, "name": "bad"}); !errors.Is(err, ErrExec) {
		t.Fatalf("Update exec error = %v, want ErrExec", err)
	}
	crudMu.Lock()
	crudExecErr = nil
	crudCountValue = "1"
	crudMu.Unlock()

	if err := table.CreateOrUpdate(map[string]any{"name": "same"}, "name"); err != nil {
		t.Fatalf("CreateOrUpdate repeat with no update fields error = %v", err)
	}
	if err := table.CreateOrUpdate(map[string]any{"name": "same", "age": 31}, "name"); err != nil {
		t.Fatalf("CreateOrUpdate repeat with update error = %v", err)
	}

	if row := table.Read(map[string]any{"id": 1}); row["name"] != "alice" {
		t.Fatalf("Read() = %#v", row)
	}
	readMap := map[string]any{"id": 1}
	if rows := table.Reads(readMap); len(rows) != 2 || readMap[IsDeleted] != 0 {
		t.Fatalf("Reads() rows=%#v readMap=%#v", rows, readMap)
	}
}

func TestTableSaveAndDataBaseStructCRUDWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	if _, err := table.Save(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("Save non-pointer error = %v, want ErrMustBeAddr", err)
	}
	empty := []crudSaveModel{}
	if rsp, err := table.Save(&empty); err != nil || rsp.ID != 0 {
		t.Fatalf("Save empty slice = %#v, %v", rsp, err)
	}

	model := crudSaveModel{Name: "alice", Age: 30}
	rsp, err := table.Save(&model, OmitFields{"age"})
	if err != nil || rsp.ID != 17 || model.ID != 17 {
		t.Fatalf("Save create rsp=%#v model=%#v err=%v", rsp, model, err)
	}
	if model.BeforeCreateCount != 1 || model.AfterCreateCount != 1 {
		t.Fatalf("Save create hooks not called: %#v", model)
	}

	model.Name = "updated"
	rsp, err = table.Save(&model)
	if err != nil || rsp.RowsAffect != 2 {
		t.Fatalf("Save update rsp=%#v err=%v", rsp, err)
	}
	if model.BeforeUpdateCount != 1 || model.AfterUpdateCount != 1 {
		t.Fatalf("Save update hooks not called: %#v", model)
	}

	models := []crudSaveModel{{Name: "a"}, {Name: "b"}}
	if _, err := table.Save(&models); err != nil {
		t.Fatalf("Save slice error = %v", err)
	}

	if _, err := db.Create(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("DataBase.Create non-pointer error = %v", err)
	}
	dbModel := crudSaveModel{Name: "db create"}
	if id, err := db.Create(&dbModel); err != nil || id != 17 || dbModel.ID != 17 {
		t.Fatalf("DataBase.Create = %d, %#v, %v", id, dbModel, err)
	}
	if _, err := db.Creates(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("DataBase.Creates non-pointer error = %v", err)
	}
	if _, err := db.Creates(&crudSaveModel{}); !errors.Is(err, ErrMustBeSlice) {
		t.Fatalf("DataBase.Creates non-slice error = %v", err)
	}
	dbModels := []crudSaveModel{{Name: "a"}, {Name: "b"}}
	if ids, err := db.Creates(&dbModels); err != nil || !reflect.DeepEqual(ids, []int{17, 17}) {
		t.Fatalf("DataBase.Creates ids=%#v err=%v", ids, err)
	}

	if _, err := db.Delete(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("DataBase.Delete non-pointer error = %v", err)
	}
	if _, err := db.Delete(&crudSaveModel{}); !errors.Is(err, ErrMustNeedID) {
		t.Fatalf("DataBase.Delete no ID error = %v", err)
	}
	if got, err := db.Delete(&crudSaveModel{ID: 1}); err != nil || got != 2 {
		t.Fatalf("DataBase.Delete = %d, %v", got, err)
	}
	if got, err := db.Deletes(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) || got != 0 {
		t.Fatalf("DataBase.Deletes non-pointer = %d, %v", got, err)
	}
	if got, err := db.Deletes(&crudSaveModel{}); !errors.Is(err, ErrMustBeSlice) || got != 0 {
		t.Fatalf("DataBase.Deletes non-slice = %d, %v", got, err)
	}
	if got, err := db.Deletes(&[]crudSaveModel{{ID: 1}, {ID: 2}}); err != nil || got != 0 {
		t.Fatalf("DataBase.Deletes current return = %d, %v", got, err)
	}

	if err := db.Update(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) {
		t.Fatalf("DataBase.Update non-pointer error = %v", err)
	}
	if err := db.Update(&crudSaveModel{ID: 1, Name: "updated"}); err != nil {
		t.Fatalf("DataBase.Update error = %v", err)
	}
	if got, err := db.Updates(crudSaveModel{}); !errors.Is(err, ErrMustBeAddr) || got != 0 {
		t.Fatalf("DataBase.Updates non-pointer = %d, %v", got, err)
	}
	if got, err := db.Updates(&crudSaveModel{}); !errors.Is(err, ErrMustBeSlice) || got != 0 {
		t.Fatalf("DataBase.Updates non-slice = %d, %v", got, err)
	}
	if got, err := db.Updates(&[]crudSaveModel{{ID: 1}, {ID: 2}}); err != nil || got != 2 {
		t.Fatalf("DataBase.Updates slice = %d, %v", got, err)
	}
}

func TestSearchResultHelpersWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	if row := table.WhereID(1).RowMap(); row["name"] != "alice" {
		t.Fatalf("Search.RowMap() = %#v", row)
	}
	if row := table.WhereID(1).RowMapInterface(); row.String("name") != "alice" {
		t.Fatalf("Search.RowMapInterface() = %#v", row)
	}
	if rows := table.Where("age > ?", 20).RowsMapInterface(); len(rows) != 2 {
		t.Fatalf("Search.RowsMapInterface() = %#v", rows)
	}
	if rows := table.Where("age > ?", 20).RowsMapNull(); len(rows) != 2 || rows[0]["name"] != "alice" {
		t.Fatalf("Search.RowsMapNull() = %#v", rows)
	}
	if cols, rows := table.Where("age > ?", 20).DoubleSlice(); cols["name"] != 1 || len(rows) != 2 {
		t.Fatalf("Search.DoubleSlice() cols=%#v rows=%#v", cols, rows)
	}
	if got := table.Fields("COUNT(*) AS total").Int("total"); got != 0 {
		t.Fatalf("Search.Int(total) = %d, want 0 from stub count", got)
	}
	crudMu.Lock()
	crudCountValue = "1"
	crudMu.Unlock()
	if got := table.Fields("COUNT(*) AS total").Bool("total"); !got {
		t.Fatalf("Search.Bool(total) = false, want true")
	}
	if got := table.Fields("name").String("name"); got != "alice" {
		t.Fatalf("Search.String(name) = %q", got)
	}
	if got := table.Fields("amount").Float("amount"); got != 2.5 {
		t.Fatalf("Search.Float(amount) = %v", got)
	}

	explain := table.WhereID(1).Explain(false)
	if explain.ID != 1 || explain.Table != "user" || explain.Key != "PRIMARY" {
		t.Fatalf("Explain() = %#v", explain)
	}
	if !strings.Contains(explain.String(), "PRIMARY") {
		t.Fatalf("Explain.String() = %q", explain.String())
	}

	var found []crudSaveModel
	if err := table.Where("age > ?", 20).Finds(&found); err != nil {
		t.Fatalf("Search.Finds() error = %v", err)
	}
	if len(found) != 2 || found[0].Name != "alice" {
		t.Fatalf("Search.Finds() = %#v", found)
	}
}

type afterFindModel struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Calls int    `mx:"-"`
}

func (m *afterFindModel) AfterFind() error {
	m.Calls++
	return nil
}

func TestModelStructSetStructAndTableStructWithStub(t *testing.T) {
	db := resetCRUDStubDB(t)
	table := db.Table("user")

	model := afterFindModel{}
	ms, err := NewModelStruct(&model)
	if err != nil {
		t.Fatalf("NewModelStruct() error = %v", err)
	}
	cols := map[string]int{"id": 0, "name": 1}
	data := [][]byte{[]byte("7"), []byte("alice")}
	if err := ms.SetStruct(table, cols, data, false); err != nil {
		t.Fatalf("SetStruct() error = %v", err)
	}
	if model.ID != 7 || model.Name != "alice" || model.Calls != 1 {
		t.Fatalf("SetStruct() model = %#v", model)
	}

	var tableModel afterFindModel
	if err := table.WhereID(1).Struct(&tableModel); err != nil {
		t.Fatalf("Table.Struct() error = %v", err)
	}
	if tableModel.Name != "alice" || tableModel.Calls != 1 {
		t.Fatalf("Table.Struct() model = %#v", tableModel)
	}

	var tableModels []afterFindModel
	if err := table.Where("age > ?", 20).ToStruct(&tableModels); err != nil {
		t.Fatalf("Table.ToStruct slice error = %v", err)
	}
	if len(tableModels) != 2 || tableModels[0].Name != "alice" || tableModels[0].Calls != 1 {
		t.Fatalf("Table.ToStruct slice = %#v", tableModels)
	}
	if err := table.Where("age > ?", 20).ToStruct(new(int)); err == nil {
		t.Fatalf("Table.ToStruct unsupported error = nil")
	}
	if err := table.Where("age > ?", 20).Struct(new(int)); err == nil {
		t.Fatalf("Table.Struct unsupported error = nil")
	}

	noQuery := table.MustIn("id")
	if err := noQuery.Struct(&tableModel); err != nil {
		t.Fatalf("Struct noNeedQuery error = %v", err)
	}
	if err := noQuery.ToStruct(&tableModel); err != nil {
		t.Fatalf("ToStruct noNeedQuery error = %v", err)
	}
}
