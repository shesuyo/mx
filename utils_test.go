package mx

import (
	"reflect"
	"testing"
	"time"
)

func TestSafeMapStringString_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name  string
		safe  *SafeMapStringString
		args  args
		want  string
		want1 bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.safe.Get(tt.args.key)
			if got != tt.want {
				t.Errorf("SafeMapStringString.Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("SafeMapStringString.Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSafeMapStringString_Set(t *testing.T) {
	type args struct {
		key string
		val string
	}
	tests := []struct {
		name string
		safe *SafeMapStringString
		args args
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.safe.Set(tt.args.key, tt.args.val)
		})
	}
}

func TestNewMapStringString(t *testing.T) {
	tests := []struct {
		name string
		want *SafeMapStringString
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMapStringString(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMapStringString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toDBNameV1(name string) string {
	if name == "" {
		return ""
	}
	dbNameBytes := []byte{}
	preCapital := false
	for i := 0; i < len(name); i++ {
		if name[i] >= 'A' && name[i] <= 'Z' {
			if !preCapital && i > 0 {
				dbNameBytes = append(dbNameBytes, '_')
			}
			dbNameBytes = append(dbNameBytes, name[i]+32)
			preCapital = true
		} else {
			dbNameBytes = append(dbNameBytes, name[i])
			preCapital = false
		}
	}
	dbName := string(dbNameBytes)
	dbNameMap.Set(name, dbName)
	dbNameMap.Set(dbName, name)
	return dbName
}

func TestToDBName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"", args{"UserWeapon"}, "user_weapon"},
		{"", args{"SN"}, "sn"},
		{"", args{"APIVersion"}, "apiversion"}, // 这里旧的会转换成 api_version，要看看数据库有没有这类字段。
		{"", args{"Version9"}, "version9"},
		{"", args{"User"}, "user"},
		{"", args{"UID"}, "uid"},
		{"", args{"UserUID"}, "user_uid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDBNameV1(tt.args.name); got != tt.want {
				t.Errorf("ToDBName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkToDBName(b *testing.B) {
	b.Run("ToDBNameV1", func(b *testing.B) {
		toDBNameV1("UserWeapon")
	})
	b.Run("ToDBName", func(b *testing.B) {
		toDBName("UserWeapon")
	})
}

func TestToStructName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{"user_weapon"}, "UserWeapon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToStructName(tt.args.name); got != tt.want {
				t.Errorf("ToStructName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toStructName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{"user_weapon"}, "UserWeapon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toStructName(tt.args.name); got != tt.want {
				t.Errorf("toStructName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toDBName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{"UserWeapon"}, "user_weapon"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toDBName(tt.args.name); got != tt.want {
				t.Errorf("toDBName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ksvs(t *testing.T) {
	type args struct {
		m       map[string]any
		keyTail []string
	}
	tests := []struct {
		name  string
		args  args
		want  []string
		want1 []any
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ksvs(tt.args.m, tt.args.keyTail...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ksvs() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ksvs() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_argslice(t *testing.T) {
	type args struct {
		l int
	}
	tests := []struct {
		name string
		args args
		want string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := argslice(tt.args.l); got != tt.want {
				t.Errorf("argslice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getTags(t *testing.T) {
	type args struct {
		v     reflect.Value
		t     reflect.Type
		table *Table
	}
	tests := []struct {
		name string
		args args
		want []string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getTags(tt.args.v, tt.args.t, tt.args.table); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_structToMap(t *testing.T) {
	type args struct {
		v     reflect.Value
		table *Table
	}
	tests := []struct {
		name string
		args args
		want map[string]any
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := structToMap(tt.args.v, tt.args.table); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("structToMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlaceholder(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{1}, "?"},
		{"", args{3}, "?,?,?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Placeholder(tt.args.n); got != tt.want {
				t.Errorf("Placeholder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_placeholder(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{1}, "?"},
		{"", args{3}, "?,?,?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := placeholder(tt.args.n); got != tt.want {
				t.Errorf("placeholder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMapsToCRUDRows(t *testing.T) {
	type args struct {
		m []map[string]string
	}
	tests := []struct {
		name string
		args args
		want RowsMap
	}{

		{"", args{m: []map[string]string{{"a": "1"}}}, RowsMap{{"a": "1"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MapsToCRUDRows(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MapsToCRUDRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWhereTimeParse(t *testing.T) {
	type args struct {
		field  string
		ts     string
		years  int
		months int
		days   int
	}
	tests := []struct {
		name string
		args args
		want string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WhereTimeParse(tt.args.field, tt.args.ts, tt.args.years, tt.args.months, tt.args.days); got != tt.want {
				t.Errorf("WhereTimeParse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeParse(t *testing.T) {
	type args struct {
		ts string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timeParse(tt.args.ts)
			if (err != nil) != tt.wantErr {
				t.Errorf("timeParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("timeParse() = %v, want %v", got, tt.want)
			}
		})
	}
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

func Test_byteString(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{"", args{[]byte{'a', 'b'}}, "ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := byteString(tt.args.b); got != tt.want {
				t.Errorf("byteString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringByte(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{

		{"", args{"ab"}, []byte{'a', 'b'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringByte(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("stringByte() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	vals := []any{
		float64((8.7 * 100.0) / 100.0),
		-1,
		0,
		1,
	}
	ans := []any{
		"8.7",
		"-1",
		"0",
		"1",
	}

	for idx, val := range vals {
		if String(val) != ans[idx] {
			t.Fatal("String", val, "want:", ans[idx])
		}
	}
}

func TestInt(t *testing.T) {
	type args struct {
		v any
	}
	tests := []struct {
		name string
		args args
		want int
	}{

		{"", args{"1"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Int(tt.args.v); got != tt.want {
				t.Errorf("Int() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_callFunc(t *testing.T) {
	type args struct {
		v    reflect.Value
		name string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := callFunc(tt.args.v, tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("callFunc() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTime_Parse(t *testing.T) {
	tests := []struct {
		name string
		t    *Time
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t.Parse()
		})
	}
}

func Test_setReflectValue(t *testing.T) {
	type args struct {
		v  reflect.Value
		bs []byte
	}
	var i int
	var s string
	var ui uint
	var is []int
	var f64 float64
	var f32 float32
	tests := []struct {
		name string
		args args
		want any
	}{
		{"", args{reflect.ValueOf(&i).Elem(), []byte("1")}, int(1)},
		{"", args{reflect.ValueOf(&s).Elem(), []byte("1")}, "1"},
		{"", args{reflect.ValueOf(&ui).Elem(), []byte("1")}, uint(1)},
		{"", args{reflect.ValueOf(&is).Elem(), []byte("[1,2]")}, []int{1, 2}},
		{"", args{reflect.ValueOf(&f64).Elem(), []byte("1")}, float64(1)},
		{"", args{reflect.ValueOf(&f32).Elem(), []byte("1")}, float32(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setReflectValue(tt.args.v, tt.args.bs)
			if !reflect.DeepEqual(tt.args.v.Interface(), tt.want) {
				t.Errorf("setReflectValue() = %v, want %v", tt.args.v.Interface(), tt.want)
			}
		})
	}
}

func Test_expandSlice(t *testing.T) {
	type args struct {
		arg any
	}
	tests := []struct {
		name string
		args args
		want []any
	}{
		{"", args{nil}, make([]any, 0)},
		{"", args{[]int{}}, make([]any, 0)},
		{"", args{[]int{1, 2, 3}}, []any{1, 2, 3}},
		{"", args{[]any{1, 2, 3}}, []any{1, 2, 3}},
		{"", args{[]string{"1", "2", "3"}}, []any{"1", "2", "3"}},
		{"", args{[]any{"1", "2", "3"}}, []any{"1", "2", "3"}},
		{"", args{1}, []any{1}},
		{"", args{"1"}, []any{"1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := expandSlice(tt.args.arg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("expandSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}
