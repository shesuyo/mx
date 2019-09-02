package mx

import (
	"reflect"
	"strconv"
	"testing"
)

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
		{"1997@", args{"1997", "1998"}, "1997-01-01 00:00:00", "1999-01-01 00:00:00", false},
		{"1997@", args{"1997-01", ""}, "1997-01-01 00:00:00", "1997-02-01 00:00:00", false},
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

type User struct {
	ID int
}

func (u *User) AfterFind() error {
	return nil
}
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
