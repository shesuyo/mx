package mx

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

// go test -coverprofile=c.out
// go tool cover -html=c.out -o coverage.html

func TestQuery1(t *testing.T) {
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
			fmt.Println(ct.Name(), ct.DatabaseTypeName(), ct.ScanType().Name(), length)
		}
		vals := make([]*sql.RawBytes, 7)
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

func TestQQQ(t *testing.T) {
	// t.Log(NewModel(User{}))
	t.Log(NewModelStruct(&User{}))
}

// Deprecated: 弃用此方法

// fmt.Println(ms.rvp.Kind(), ms.rvp.CanAddr(), ms.rvp.Elem().CanAddr())

func TestNewModelStruct(t *testing.T) {
	v := User{}
	ms, err := NewModelStruct(v)
	t.Log(ms, err)

	tests := []interface{}{
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
	Gem            []Gem  `json:"gem"`
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if af := u.MethodByName(AfterFind); af.IsValid() {
			af.Call(nil)
		}
	}
}

func BenchmarkAssertionFunc(b *testing.B) {
	var u interface{} = &User{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < 20; i++ {
		m["field"+strconv.Itoa(i)] = Columns{}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m["field19"]
	}
}

type KeyWithColumns struct {
	key  string
	cols Columns
}

func BenchmarkSliceGet0(b *testing.B) {
	m := []KeyWithColumns{}
	for i := 0; i < 20; i++ {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 20; j++ {
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

var db, _ = NewDataBase("root:WOaini123@tcp(localhost:3306)/demo?charset=utf8mb4")
var UserTable = db.Table("user")

func BenchmarkClone(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = UserTable.Clone()
	}
}

func TestQuery(t *testing.T) {
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
	u := User{}
	UserTable.Where("id = ?", 2).ToStruct(&u)
	if u.AfterFindCount != 1 {
		t.Fatal("AfterFind Err")
	}
}

// BenchmarkReflectAStruct-16    	    3122	    383770 ns/op	   15140 B/op	     389 allocs/op 加载一个结构体和一个slice
// BenchmarkReflectAStruct-16    	    9615	    117831 ns/op	    3676 B/op	      98 allocs/op
func BenchmarkReflectAStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = UserTable.Where("id = ?", 2).ToStruct(&User{})
	}
}

// has one
// has many
// many to many

func isSlice(args interface{}) bool {
	return reflect.TypeOf(args).Kind() == reflect.Slice
}

func TestIsSlice(t *testing.T) {
	args := []interface{}{}
	if !isSlice(args) {
		t.Fatal("fail with is slice")
	}
}

func BenchmarkIsSlice(b *testing.B) {
	args := []interface{}{}
	for i := 0; i < b.N; i++ {
		isSlice(args)
	}
}

func JSONStringify(v interface{}) string {
	bs, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(bs)
}
