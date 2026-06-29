package mx

import (
	"database/sql"
	"reflect"
	"strconv"
	"testing"
)

// go test -run=^$ -bench "^(BenchmarkReflectFunc|BenchmarkAssertionFunc)$" -benchmem -count=5
// goos: windows
// goarch: amd64
// pkg: github.com/shesuyo/mx
// cpu: 13th Gen Intel(R) Core(TM) i7-13700K
// BenchmarkReflectFunc-24          4350504               281.6 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4272934               281.9 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4390539               278.7 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4406395               275.7 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4313017               278.6 ns/op           136 B/op          5 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8620 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8652 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8759 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8607 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8632 ns/op          0 B/op          0 allocs/op
// avg: reflect 279.3 ns/op, assertion 0.8654 ns/op
// PASS
// ok      github.com/shesuyo/mx   11.051s

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
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 20 {
			if m[j].key == "field10" {
				break
			}
		}
	}
}
func BenchmarkSliceGet19(b *testing.B) {
	m := []KeyWithColumns{}
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 20 {
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

// 1s 1_000ms 1_000_000us 1_000_000_000ns

// BenchmarkQuery-16    	   10000	    109262 ns/op	    1624 B/op	      41 allocs/op
func BenchmarkQuery(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		sr := UserTable.Query("SELECT * FROM user WHERE id = ?", 2)
		for sr.rows.Next() {
			vals := make([]*sql.RawBytes, 7)
			for i := range 7 {
				vals[i] = &sql.RawBytes{}
			}
			sr.Scan(&vals)
		}
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

func BenchmarkIsSlice(b *testing.B) {
	args := []any{}
	for i := 0; i < b.N; i++ {
		isSlice(args)
	}
}
