package mx

import (
	"database/sql"
	"reflect"
	"strconv"
	"testing"
)

type User struct {
	DefaultTime
	ID       uint32 `json:"id"`
	Name     string `json:"name"`
	Age      int    `json:"age"`
	UID      int    `json:"uid"`
	Bag      Bag    `json:"bag"`
	IgnoreMe int    `mx:"-" json:"ignore_me"`
	// Weapon   Weapon `json:"weapon"`
	// Gem      []Gem  `json:"gem"`
}

type Weapon struct {
	ID     int    `json:"id"`
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
	Lv     string `json:"lv"`
	DefaultTime
}

type Gem struct {
	ID      int       `json:"id"`
	UserID  int       `json:"user_id"`
	Name    string    `json:"name"`
	Lv      string    `json:"lv"`
	History []History `json:"history"`
	DefaultTime
}
type History struct {
	ID     int    `json:"id"`
	Remark string `json:"remark"`
}

func (g *Gem) AfterFind() error {
	// fmt.Println("gem after find")
	return nil
}

func (u *User) AfterFind() error {
	// u.UID++
	// fmt.Println("u.UID++")
	return nil
}

type DefaultTime struct {
	Ctime string `json:"ctime"`
	Utime string `json:"utime"`
}

type Bag struct {
	Sn   string `json:"sn"`
	Name string `json:"name"`
}

func Test_periodParse(t *testing.T) {
	type args struct {
		st string
		et string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"", args{"", ""}, "", "", true},
		{"", args{"你好吗", ""}, "", "", true},
		{"", args{"", "1997"}, "", "", true},
		{"", args{"1997-0", ""}, "", "", true},
		{"", args{"1997-1", ""}, "", "", true},
		{"", args{"1997-1-1", ""}, "", "", true},
		{"", args{"1997-1-11", ""}, "", "", true},
		{"1997@", args{"1997", ""}, "1997-01-01 00:00:00", "1998-01-01 00:00:00", false},
		{"1997@", args{"1997", "1997"}, "1997-01-01 00:00:00", "1998-01-01 00:00:00", false},
		{"1997@", args{"1997", "1998"}, "1997-01-01 00:00:00", "1999-01-01 00:00:00", false},
		{"1997@", args{"1997-01", ""}, "1997-01-01 00:00:00", "1997-02-01 00:00:00", false},
		{"1997@", args{"1997-01", "1997-01"}, "1997-01-01 00:00:00", "1997-02-01 00:00:00", false},
		{"1997@", args{"1997-01", "1997-02"}, "1997-01-01 00:00:00", "1997-03-01 00:00:00", false},
		{"1997@", args{"1997-01-01", ""}, "1997-01-01 00:00:00", "1997-01-02 00:00:00", false},
		{"1997@", args{"1997-01-01", "1997-01-02"}, "1997-01-01 00:00:00", "1997-01-03 00:00:00", false},
		{"1997@", args{"1997-01-01 01", ""}, "1997-01-01 01:00:00", "1997-01-01 02:00:00", false},
		{"1997@", args{"1997-01-01 01", "1997-01-01 02"}, "1997-01-01 01:00:00", "1997-01-01 03:00:00", false},
		{"1997@", args{"1997-01-01 01:01", ""}, "1997-01-01 01:01:00", "1997-01-01 01:02:00", false},
		{"1997@", args{"1997-01-01 01:01", "1997-01-01 01:02"}, "1997-01-01 01:01:00", "1997-01-01 01:03:00", false},
		{"1997@", args{"1997-01-01 01:01:01", ""}, "1997-01-01 01:01:01", "1997-01-01 01:01:02", false},
		{"1997@", args{"1997-01-01 01:01:01", "1997-01-01 01:01:02"}, "1997-01-01 01:01:01", "1997-01-01 01:01:03", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := periodParse(tt.args.st, tt.args.et)
			if (err != nil) != tt.wantErr {
				t.Errorf("periodParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("periodParse() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("periodParse() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
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
	t.Fail()
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

// BenchmarkReflectAStruct-16    	    3122	    383770 ns/op	   15140 B/op	     389 allocs/op 加载一个结构体和一个slice
// BenchmarkReflectAStruct-16    	    9615	    117831 ns/op	    3676 B/op	      98 allocs/op
func BenchmarkReflectAStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = UserTable.Where("id = ?", 2).ToStruct(&User{})
	}
}
