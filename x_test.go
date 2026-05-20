package mx

import (
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
)

// go test -coverprofile=c.out
// go tool cover -html=c.out -o coverage.html

var (
	testDSN   = os.Getenv("MX_DSN")
	testDBErr error
	db        *DataBase
	UserTable *Table
)

func init() {
	if testDSN == "" || os.Getenv("MX_INTEGRATION") != "1" {
		return
	}
	db, testDBErr = NewDataBase(testDSN)
	if testDBErr != nil {
		return
	}
	if testDBErr = prepareIntegrationFixture(db); testDBErr != nil {
		return
	}
	if err := db.DB().Close(); err != nil {
		testDBErr = err
		return
	}
	db, testDBErr = NewDataBase(testDSN)
	if testDBErr != nil {
		return
	}
	UserTable = db.Table("user")
}

func requireIntegrationUserTable(tb testing.TB) *Table {
	tb.Helper()
	if os.Getenv("MX_INTEGRATION") != "1" {
		tb.Skip("set MX_INTEGRATION=1 and MX_DSN to run database integration tests")
	}
	if testDSN == "" {
		tb.Skip("MX_DSN is not set")
	}
	if testDBErr != nil {
		tb.Skipf("database integration test setup failed: %v", testDBErr)
	}
	if UserTable == nil {
		tb.Skip("user table is not available")
	}
	return UserTable
}

func requireUserColumns(tb testing.TB, columns ...string) *Table {
	tb.Helper()
	table := requireIntegrationUserTable(tb)
	missing := []string{}
	for _, column := range columns {
		if !table.HaveColumn(column) {
			missing = append(missing, column)
		}
	}
	if len(missing) > 0 {
		tb.Skipf("user table is missing test fixture columns: %v", missing)
	}
	return table
}

func TestQuery1(t *testing.T) {
	requireIntegrationUserTable(t)
	sr := UserTable.Query("SELECT * FROM user WHERE id = ?", 2)
	for sr.rows.Next() {
		vals := make([]*sql.RawBytes, 7)
		for i := 0; i < 7; i++ {
			vals[i] = &sql.RawBytes{}
		}
		t.Log(sr.Scan(&vals))
		t.Log(vals)
		for _, val := range vals {
			t.Log(val)
		}
	}
}

func TestQuery2(t *testing.T) {
	requireIntegrationUserTable(t)
	sr := UserTable.Query("SELECT * FROM user WHERE id IN (?,?,?)", 1, 2, 3)
	for sr.rows.Next() {
		cols, err := sr.rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		t.Log(cols)
		cts, err := sr.rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		for _, ct := range cts {
			length, _ := ct.Length()
			t.Log(ct.Name(), ct.DatabaseTypeName(), ct.ScanType().Name(), length)
		}
		vals := make([]*sql.RawBytes, len(cols))
		for i := 0; i < len(cols); i++ {
			vals[i] = &sql.RawBytes{}
		}
		t.Log(sr.Scan(&vals))
		t.Log(vals)
		for _, val := range vals {
			t.Log(val)
		}
	}
}

// IN 使用的时候有两种情况
// 第一种 IN长度为0的时候应该查询所有数据
// 第二种 IN长度为0的时候应该查询不到数据

// Deprecated: 弃用此方法

// fmt.Println(ms.rvp.Kind(), ms.rvp.CanAddr(), ms.rvp.Elem().CanAddr())

func TestNewModelStruct(t *testing.T) {
	v := User{}
	ms, err := NewModelStruct(v)
	t.Log(ms, err)

	tests := []any{
		User{},
	}

	for _, test := range tests {
		_ = test
	}
}

type User struct {
	DefaultTime    `json:"default_time"`
	ID             uint32 `json:"id"`
	Name           string `json:"name"`
	Age            int    `json:"age"`
	UID            int    `json:"uid"`
	IgnoreMe       int    `mx:"-" json:"ignore_me"`
	AfterFindCount int    `mx:"-" json:"after_find_count"`
	Weapon         Weapon `json:"weapon"`
	Gems           []Gem  `json:"gem"`
}

type Weapon struct {
	ID     int    `json:"id"`
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
	Lv     string `json:"lv"`
	DefaultTime
}

type Gem struct {
	ID             int       `json:"id"`
	UserID         int       `json:"user_id"`
	Name           string    `json:"name"`
	Lv             string    `json:"lv"`
	AfterFindCount int       `mx:"-" json:"after_find_count"`
	History        []History `json:"history"`
	DefaultTime
}

type History struct {
	ID     int    `json:"id"`
	Remark string `json:"remark"`
}

func (g *Gem) AfterFind() error {
	g.AfterFindCount++
	return nil
}

func (u *User) AfterFind() error {
	u.AfterFindCount++
	return nil
}

type DefaultTime struct {
	Ctime string `json:"ctime"`
	Utime string `json:"utime"`
}

// go test -benchmem -bench "^(BenchmarkReflectFunc)|(BenchmarkAssertionFunc)$"
// goos: windows
// goarch: amd64
// pkg: github.com/shesuyo/mx
// Benchmark_ReflectFunc-16         3000000               453 ns/op             208 B/op          7 allocs/op
// Benchmark_AssertionFunc-16      200000000                6.98 ns/op            0 B/op          0 allocs/op
// PASS
// ok      github.com/shesuyo/mx   3.951s

func BenchmarkReflectFunc(b *testing.B) {
	u := reflect.ValueOf(&User{})

	for b.Loop() {
		if af := u.MethodByName(AfterFind); af.IsValid() {
			af.Call(nil)
		}
	}
}

func BenchmarkAssertionFunc(b *testing.B) {
	var u any = &User{}

	for b.Loop() {
		if af, ok := u.(AfterFinder); ok {
			af.AfterFind()
		}
	}
}

// go test -benchmem -bench "^(BenchmarkMapGet)|(BenchmarkSliceGet.{1,2})$"
// goos: windows
// goarch: amd64
// pkg: github.com/shesuyo/mx
// BenchmarkMapGet-16              100000000               11.1 ns/op             0 B/op          0 allocs/op
// BenchmarkSliceGet0-16           2000000000               0.84 ns/op            0 B/op          0 allocs/op
// BenchmarkSliceGet10-16          300000000                5.05 ns/op            0 B/op          0 allocs/op
// BenchmarkSliceGet19-16          100000000               11.8 ns/op             0 B/op          0 allocs/op
// PASS
// ok      github.com/shesuyo/mx   6.167s

func BenchmarkMapGet(b *testing.B) {
	m := make(map[string]Columns)
	for i := range 20 {
		m["field"+strconv.Itoa(i)] = Columns{}
	}

	for b.Loop() {
		_ = m["field19"]
	}
}

type KeyWithColumns struct {
	key  string
	cols Columns
}

func BenchmarkSliceGet0(b *testing.B) {
	m := []KeyWithColumns{}
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}

	for b.Loop() {
		for j := range 20 {
			if m[j].key == "field0" {
				break
			}
		}
	}
}

func BenchmarkSliceGet10(b *testing.B) {
	m := []KeyWithColumns{}
	for i := 0; i < 20; i++ {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 20; j++ {
			if m[j].key == "field10" {
				break
			}
		}
	}
}
func BenchmarkSliceGet19(b *testing.B) {
	m := []KeyWithColumns{}
	for i := 0; i < 20; i++ {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 20; j++ {
			if m[j].key == "field19" {
				break
			}
		}
	}
}

func BenchmarkClone(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		_ = UserTable.Clone()
	}
}

func TestQuery(t *testing.T) {
	requireIntegrationUserTable(t)
	sr := UserTable.Query("SELECT * FROM user WHERE id = ?", 2)
	for sr.rows.Next() {
		vals := make([]*sql.RawBytes, 7)
		for i := 0; i < 7; i++ {
			vals[i] = &sql.RawBytes{}
		}
		t.Log(sr.Scan(&vals))
		t.Log(vals)
	}
}

// 1s 1_000ms 1_000_000us 1_000_000_000ns

// BenchmarkQuery-16    	   10000	    109262 ns/op	    1624 B/op	      41 allocs/op
func BenchmarkQuery(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		sr := UserTable.Query("SELECT * FROM user WHERE id = ?", 2)
		for sr.rows.Next() {
			vals := make([]*sql.RawBytes, 7)
			for i := 0; i < 7; i++ {
				vals[i] = &sql.RawBytes{}
			}
			sr.Scan(&vals)
		}
	}
}

func TestTableToStruct(t *testing.T) {
	requireIntegrationUserTable(t)
	u := User{}
	UserTable.Where("id = ?", 2).ToStruct(&u)
	if u.AfterFindCount != 1 {
		t.Fatal("AfterFind Err")
	}
}

// BenchmarkReflectAStruct-16    	    3122	    383770 ns/op	   15140 B/op	     389 allocs/op 加载一个结构体和一个slice
// BenchmarkReflectAStruct-16    	    9615	    117831 ns/op	    3676 B/op	      98 allocs/op
func BenchmarkReflectAStruct(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		_ = UserTable.Where("id = ?", 2).ToStruct(&User{})
	}
}

// has one
// has many
// many to many

func isSlice(args any) bool {
	return reflect.TypeOf(args).Kind() == reflect.Slice
}

func TestIsSlice(t *testing.T) {
	args := []any{}
	if !isSlice(args) {
		t.Fatal("fail with is slice")
	}
}

func BenchmarkIsSlice(b *testing.B) {
	args := []any{}
	for i := 0; i < b.N; i++ {
		isSlice(args)
	}
}

func JSONStringify(v any) string {
	bs, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(bs)
}

func TestToStructSliceNested(t *testing.T) {
	requireIntegrationUserTable(t)
	us := []User{}
	UserTable.DataBase.debug = true
	UserTable.Where("id IN(1,2)").ToStruct(&us)
	t.Log("user len:", len(us))

	for _, u := range us {
		t.Log(u.ID, u.Name, u.Weapon.Name)
		t.Log("gems len:", len(u.Gems))
	}
}

// TestWhere 测试 Where 方法
func TestWhere(t *testing.T) {
	requireUserColumns(t, "age")
	users := UserTable.Where("age > ?", 18).RowsMap()
	t.Logf("Found %d users with age > 18", len(users))
}

// TestIn 测试 In 方法
func TestIn(t *testing.T) {
	requireIntegrationUserTable(t)
	users := UserTable.In("id", 1, 2, 3).RowsMap()
	t.Logf("Found %d users with id IN (1,2,3)", len(users))
}

// TestNotIn 测试 NotIn 方法
func TestNotIn(t *testing.T) {
	requireIntegrationUserTable(t)
	users := UserTable.NotIn("id", 1, 2).RowsMap()
	t.Logf("Found %d users with id NOT IN (1,2)", len(users))
}

// TestOrderBy 测试 OrderBy 方法
func TestOrderBy(t *testing.T) {
	requireUserColumns(t, "age")
	users := UserTable.OrderBy("age", true).Limit(5).RowsMap()
	if len(users) > 0 {
		t.Logf("First user age: %s", users[0]["age"])
	}
}

// TestLimitOffset 测试 Limit 和 Offset 方法
func TestLimitOffset(t *testing.T) {
	requireIntegrationUserTable(t)
	users := UserTable.Limit(5).Offset(2).RowsMap()
	t.Logf("Found %d users with LIMIT 5 OFFSET 2", len(users))
}

// TestPage 测试 Page 方法
func TestPage(t *testing.T) {
	requireIntegrationUserTable(t)
	users := UserTable.Page(10, 2).RowsMap()
	t.Logf("Found %d users on page 2 with limit 10", len(users))
}

// TestFields 测试 Fields 方法
func TestFields(t *testing.T) {
	requireUserColumns(t, "id", "name", "age")
	users := UserTable.Fields("id", "name", "age").Limit(3).RowsMap()
	if len(users) > 0 {
		// 验证只返回了指定的字段
		for k := range users[0] {
			if k != "id" && k != "name" && k != "age" {
				t.Errorf("Unexpected field: %s", k)
			}
		}
	}
}

// TestCount 测试 Count 方法
func TestCount(t *testing.T) {
	requireIntegrationUserTable(t)
	count := UserTable.Count()
	t.Logf("Total users: %d", count)
}

// TestGroupBy 测试 GroupBy 方法
func TestGroupBy(t *testing.T) {
	requireUserColumns(t, "age")
	results := UserTable.Fields("age", "COUNT(*) AS total").Group("age").RowsMap()
	t.Logf("Found %d age groups", len(results))
	for _, r := range results {
		t.Logf("Age: %s, Count: %s", r["age"], r["total"])
	}
}

// TestWhereLike 测试 WhereLike 方法
func TestWhereLike(t *testing.T) {
	requireUserColumns(t, "name")
	users := UserTable.WhereLike("name", "test").Limit(5).RowsMap()
	t.Logf("Found %d users with name containing 'test'", len(users))
}

// TestWhereLikeLeft 测试 WhereLikeLeft 方法
func TestWhereLikeLeft(t *testing.T) {
	requireUserColumns(t, "name")
	users := UserTable.WhereLikeLeft("name", "john").Limit(5).RowsMap()
	t.Logf("Found %d users with name ending with 'john'", len(users))
}

// TestWhereLikeRight 测试 WhereLikeRight 方法
func TestWhereLikeRight(t *testing.T) {
	requireUserColumns(t, "name")
	users := UserTable.WhereLikeRight("name", "john").Limit(5).RowsMap()
	t.Logf("Found %d users with name starting with 'john'", len(users))
}

// TestMustIn 测试 MustIn 方法（空参数应返回空结果）
func TestMustIn(t *testing.T) {
	requireIntegrationUserTable(t)
	users := UserTable.MustIn("id").RowsMap()
	if len(users) != 0 {
		t.Errorf("MustIn with empty args should return empty result, got %d", len(users))
	}
}

// TestJoins 测试 Joins 方法
func TestJoins(t *testing.T) {
	requireUserColumns(t, "id", "name")
	if !UserTable.DataBase.HaveTable("weapon") {
		t.Skip("weapon table is not available")
	}
	// 假设有 weapon 表
	users := UserTable.Joins("weapon").Fields("user.id", "user.name", "weapon.name AS weapon_name").Limit(5).RowsMap()
	t.Logf("Found %d users with weapon join", len(users))
	for _, u := range users {
		t.Logf("User: %s, Weapon: %s", u["name"], u["weapon_name"])
	}
}

// TestCreate 测试 Create 方法
func TestCreate(t *testing.T) {
	requireUserColumns(t, "name", "age", "uid")
	data := map[string]any{
		"name": "test_user",
		"age":  25,
		"uid":  9999,
	}
	id, err := UserTable.Create(data)
	if err != nil {
		t.Errorf("Create failed: %v", err)
		return
	}
	t.Logf("Created user with ID: %d", id)

	// 清理测试数据
	UserTable.DeleteID(id)
}

// TestUpdate 测试 Update 方法
func TestUpdate(t *testing.T) {
	requireUserColumns(t, "name", "age", "uid")
	// 先创建一个测试用户
	data := map[string]any{
		"name": "update_test",
		"age":  30,
		"uid":  9998,
	}
	id, err := UserTable.Create(data)
	if err != nil {
		t.Skipf("Skip test, cannot create test user: %v", err)
		return
	}

	// 更新用户
	updateData := map[string]any{
		"id":   id,
		"name": "updated_user",
		"age":  35,
	}
	affected, err := UserTable.Update(updateData)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	} else {
		t.Logf("Update affected %d rows", affected)
	}

	// 清理
	UserTable.DeleteID(id)
}

// TestDelete 测试 Delete 方法
func TestDelete(t *testing.T) {
	requireUserColumns(t, "name", "age", "uid")
	// 先创建一个测试用户
	data := map[string]any{
		"name": "delete_test",
		"age":  40,
		"uid":  9997,
	}
	id, err := UserTable.Create(data)
	if err != nil {
		t.Skipf("Skip test, cannot create test user: %v", err)
		return
	}

	// 删除用户
	affected, err := UserTable.Delete(map[string]any{"id": id})
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	} else {
		t.Logf("Delete affected %d rows", affected)
	}
}

// TestRead 测试 Read 方法
func TestRead(t *testing.T) {
	requireIntegrationUserTable(t)
	user := UserTable.Read(map[string]any{"id": 1})
	if user.HaveRecord() {
		t.Logf("Found user: %s", user["name"])
	} else {
		t.Log("No user found with id=1")
	}
}

// TestReads 测试 Reads 方法
func TestReads(t *testing.T) {
	requireUserColumns(t, "age")
	users := UserTable.Reads(map[string]any{"age": 25})
	t.Logf("Found %d users with age=25", len(users))
}

// TestIDIn 测试 IDIn 方法
func TestIDIn(t *testing.T) {
	requireIntegrationUserTable(t)
	sr := UserTable.IDIn(1, 2, 3)
	defer sr.rows.Close()

	count := 0
	for sr.rows.Next() {
		count++
	}
	t.Logf("IDIn found %d rows", count)
}

// TestIDRow 测试 IDRow 方法
func TestIDRow(t *testing.T) {
	requireIntegrationUserTable(t)
	user := UserTable.IDRow(1)
	if user.HaveRecord() {
		t.Logf("IDRow found user: %s", user["name"])
	} else {
		t.Log("IDRow: No user found with id=1")
	}
}

// TestMaxID 测试 MaxID 方法
func TestMaxID(t *testing.T) {
	requireIntegrationUserTable(t)
	maxID := UserTable.MaxID()
	t.Logf("Max ID in user table: %d", maxID)
}

// TestAutoIncrement 测试 AutoIncrement 方法
func TestAutoIncrement(t *testing.T) {
	requireIntegrationUserTable(t)
	autoInc := UserTable.AutoIncrement()
	t.Logf("Auto increment value: %d", autoInc)
}

// TestClone 测试 Clone 方法
func TestClone(t *testing.T) {
	requireIntegrationUserTable(t)
	cloned := UserTable.Clone()
	if cloned.Name() != UserTable.Name() {
		t.Error("Cloned table has different name")
	}
	cloned.Where("age > ?", 18)
	// 原表不应受到影响
	t.Log("Clone test passed")
}

// TestSaveCreate 测试 Save 方法（创建）
func TestSaveCreate(t *testing.T) {
	requireUserColumns(t, "name", "age", "uid")
	user := User{
		Name: "save_test_create",
		Age:  28,
		UID:  9996,
	}
	resp, err := UserTable.Save(&user)
	if err != nil {
		t.Errorf("Save create failed: %v", err)
		return
	}
	t.Logf("Save created user with ID: %d", resp.ID)

	// 清理
	UserTable.DeleteID(resp.ID)
}

// TestSaveUpdate 测试 Save 方法（更新）
func TestSaveUpdate(t *testing.T) {
	requireUserColumns(t, "name", "age", "uid")
	// 先创建一个用户
	user := User{
		Name: "save_test_update",
		Age:  29,
		UID:  9995,
	}
	resp, err := UserTable.Save(&user)
	if err != nil {
		t.Skipf("Skip test, cannot create test user: %v", err)
		return
	}

	// 更新用户
	user.Name = "updated_by_save"
	user.Age = 31
	resp, err = UserTable.Save(&user)
	if err != nil {
		t.Errorf("Save update failed: %v", err)
	} else {
		t.Logf("Save updated user, rows affected: %d", resp.RowsAffect)
	}

	// 清理
	UserTable.DeleteID(user.ID)
}

// TestToStruct 测试 ToStruct 方法（单个结构体）
func TestToStruct(t *testing.T) {
	requireIntegrationUserTable(t)
	var user User
	err := UserTable.Where("id = ?", 1).ToStruct(&user)
	if err != nil {
		t.Errorf("ToStruct failed: %v", err)
		return
	}
	t.Logf("ToStruct found user: %s (age: %d)", user.Name, user.Age)
}

// TestToStructSlice 测试 ToStruct 方法（切片）
func TestToStructSlice(t *testing.T) {
	requireUserColumns(t, "age")
	var users []User
	err := UserTable.Where("age > ?", 18).Limit(5).ToStruct(&users)
	if err != nil {
		t.Errorf("ToStruct slice failed: %v", err)
		return
	}
	t.Logf("ToStruct found %d users", len(users))
}

// TestSearchInt 测试 Int 方法
func TestSearchInt(t *testing.T) {
	requireIntegrationUserTable(t)
	count := UserTable.Fields("COUNT(*) AS total").Int()
	t.Logf("Count result: %d", count)
}

// TestSearchString 测试 String 方法
func TestSearchString(t *testing.T) {
	requireUserColumns(t, "name")
	name := UserTable.Where("id = ?", 1).Fields("name").String()
	t.Logf("String result: %s", name)
}

// TestFloat 测试 Float 方法
func TestFloat(t *testing.T) {
	requireUserColumns(t, "age")
	// 假设有一个 float 类型的字段
	avgAge := UserTable.Fields("AVG(age) AS avg_age").Float("avg_age")
	t.Logf("Average age: %f", avgAge)
}

// TestBool 测试 Bool 方法
func TestBool(t *testing.T) {
	requireIntegrationUserTable(t)
	hasUsers := UserTable.Fields("COUNT(*) AS total").Bool()
	t.Logf("Has users: %v", hasUsers)
}

// TestRowMap 测试 RowMap 方法
func TestRowMap(t *testing.T) {
	requireUserColumns(t, "id", "name", "age")
	row := UserTable.Where("id = ?", 1).Fields("id", "name", "age").RowMap()
	if row.HaveRecord() {
		t.Logf("RowMap: id=%s, name=%s, age=%s", row["id"], row["name"], row["age"])
	}
}

// TestRowsMap 测试 RowsMap 方法
func TestRowsMap(t *testing.T) {
	requireIntegrationUserTable(t)
	rows := UserTable.Limit(5).RowsMap()
	t.Logf("RowsMap found %d rows", len(rows))
}

// TestRowMapInterface 测试 RowMapInterface 方法
func TestRowMapInterface(t *testing.T) {
	requireIntegrationUserTable(t)
	row := UserTable.Where("id = ?", 1).RowMapInterface()
	t.Logf("RowMapInterface: %v", row)
}

// TestRowsMapInterface 测试 RowsMapInterface 方法
func TestRowsMapInterface(t *testing.T) {
	requireIntegrationUserTable(t)
	rows := UserTable.Limit(5).RowsMapInterface()
	t.Logf("RowsMapInterface found %d rows", len(rows))
}

// TestDoubleSlice 测试 DoubleSlice 方法
func TestDoubleSlice(t *testing.T) {
	requireIntegrationUserTable(t)
	cols, data := UserTable.Limit(5).DoubleSlice()
	t.Logf("DoubleSlice: %d columns, %d rows", len(cols), len(data))
}

// TestRowMapMethods 测试 RowMap 方法
func TestRowMapMethods(t *testing.T) {
	requireUserColumns(t, "name", "age")
	row := UserTable.Where("id = ?", 1).RowMap()

	// 测试 Int 方法
	age := row.Int("age")
	t.Logf("Age from Int(): %d", age)

	// 测试 Float64 方法
	ageFloat := row.Float64("age")
	t.Logf("Age from Float64(): %f", ageFloat)

	// 测试 HaveRecord 方法
	if row.HaveRecord() {
		t.Log("HaveRecord: true")
	}

	// 测试 NotFound 方法
	if !row.NotFound() {
		t.Log("NotFound: false")
	}

	// 测试 Copy 方法
	copied := row.Copy()
	t.Logf("Copied row has %d fields", len(copied))

	// 测试 FieldDefault 方法
	unknown := row.FieldDefault("unknown_field", "default_value")
	t.Logf("FieldDefault for unknown field: %s", unknown)
}

// TestRowsMapMethods 测试 RowsMap 方法
func TestRowsMapMethods(t *testing.T) {
	requireUserColumns(t, "name", "age")
	rows := UserTable.Limit(10).RowsMap()

	// 测试 Len 方法
	t.Logf("RowsMap Len: %d", rows.Len())

	// 测试 First 方法
	first := rows.First()
	if first.HaveRecord() {
		t.Logf("First row name: %s", first["name"])
	}

	// 测试 PluckInt 方法
	ages := rows.PluckInt("age")
	t.Logf("Plucked %d ages", len(ages))

	// 测试 PluckString 方法
	names := rows.PluckString("name")
	t.Logf("Plucked %d names", len(names))

	// 测试 Sum 方法
	totalAge := rows.Sum("age")
	t.Logf("Sum of ages: %d", totalAge)

	// 测试 MapIndex 方法
	byAge := rows.MapIndex("age")
	t.Logf("Grouped by age: %d groups", len(byAge))

	// 测试 Unique 方法
	uniqueNames := rows.Unique("name")
	t.Logf("Unique names: %d", len(uniqueNames))

	// 测试 Filter 方法
	filtered := rows.Filter("age", "25")
	t.Logf("Filtered by age=25: %d rows", len(filtered))
}

// TestRowsMapSort 测试 RowsMap 排序方法
func TestRowsMapSort(t *testing.T) {
	requireUserColumns(t, "name", "age")
	rows := UserTable.Limit(10).RowsMap()

	// 测试 Sort 方法
	sorted := rows.Sort("name", false)
	t.Logf("Sorted %d rows by name ASC", len(*sorted))

	// 测试 SortInt 方法
	sortedInt := rows.SortInt("age", true)
	t.Logf("Sorted %d rows by age DESC", len(*sortedInt))
}

// TestParse 测试 Parse 方法
func TestParse(t *testing.T) {
	requireUserColumns(t, "age")
	query, args := UserTable.Where("age > ?", 18).Limit(5).Parse()
	t.Logf("Parsed query: %s", query)
	t.Logf("Parsed args: %v", args)
}

// TestExplain 测试 Explain 方法
func TestExplain(t *testing.T) {
	requireIntegrationUserTable(t)
	explain := UserTable.Where("id = ?", 1).Explain(false)
	t.Logf("Explain result: %+v", explain)
}
