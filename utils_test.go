package mx

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"sync"
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
	type stringAlias string
	type int64Alias int64
	type bytesAlias []byte

	type args struct {
		v  reflect.Value
		bs []byte
	}
	var i int
	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var s string
	var sa stringAlias
	var ia int64Alias
	var ui uint
	var ui8 uint8
	var ui16 uint16
	var ui32 uint32
	var ui64 uint64
	var b bool
	var is []int
	var bs []byte
	var bas bytesAlias
	var f64 float64
	var f32 float32
	var ip *int
	var sp *string
	var bp *bool
	var tm time.Time
	var tmp *time.Time
	var nt sql.NullTime
	tests := []struct {
		name string
		args args
		want any
	}{
		{"int", args{reflect.ValueOf(&i).Elem(), []byte("1")}, int(1)},
		{"int8", args{reflect.ValueOf(&i8).Elem(), []byte("1")}, int8(1)},
		{"int16", args{reflect.ValueOf(&i16).Elem(), []byte("1")}, int16(1)},
		{"int32", args{reflect.ValueOf(&i32).Elem(), []byte("1")}, int32(1)},
		{"int64", args{reflect.ValueOf(&i64).Elem(), []byte("1")}, int64(1)},
		{"string", args{reflect.ValueOf(&s).Elem(), []byte("1")}, "1"},
		{"string alias", args{reflect.ValueOf(&sa).Elem(), []byte("ok")}, stringAlias("ok")},
		{"int64 alias", args{reflect.ValueOf(&ia).Elem(), []byte("2")}, int64Alias(2)},
		{"uint", args{reflect.ValueOf(&ui).Elem(), []byte("1")}, uint(1)},
		{"uint8", args{reflect.ValueOf(&ui8).Elem(), []byte("1")}, uint8(1)},
		{"uint16", args{reflect.ValueOf(&ui16).Elem(), []byte("1")}, uint16(1)},
		{"uint32", args{reflect.ValueOf(&ui32).Elem(), []byte("1")}, uint32(1)},
		{"uint64", args{reflect.ValueOf(&ui64).Elem(), []byte("1")}, uint64(1)},
		{"bool", args{reflect.ValueOf(&b).Elem(), []byte("true")}, true},
		{"slice", args{reflect.ValueOf(&is).Elem(), []byte("[1,2]")}, []int{1, 2}},
		{"bytes", args{reflect.ValueOf(&bs).Elem(), []byte("abc")}, []byte("abc")},
		{"bytes alias", args{reflect.ValueOf(&bas).Elem(), []byte("abc")}, bytesAlias("abc")},
		{"float64", args{reflect.ValueOf(&f64).Elem(), []byte("1")}, float64(1)},
		{"float32", args{reflect.ValueOf(&f32).Elem(), []byte("1")}, float32(1)},
		{"int pointer", args{reflect.ValueOf(&ip).Elem(), []byte("1")}, int(1)},
		{"string pointer", args{reflect.ValueOf(&sp).Elem(), []byte("ptr")}, "ptr"},
		{"bool pointer", args{reflect.ValueOf(&bp).Elem(), []byte("1")}, true},
		{"time", args{reflect.ValueOf(&tm).Elem(), []byte("2026-05-13 12:30:45")}, time.Date(2026, 5, 13, 12, 30, 45, 0, time.Local)},
		{"time pointer", args{reflect.ValueOf(&tmp).Elem(), []byte("2026-05-13")}, time.Date(2026, 5, 13, 0, 0, 0, 0, time.Local)},
		{"sql null time", args{reflect.ValueOf(&nt).Elem(), []byte("2026-05-13 12:30:45")}, sql.NullTime{Time: time.Date(2026, 5, 13, 12, 30, 45, 0, time.Local), Valid: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setReflectValue(tt.args.v, tt.args.bs); err != nil {
				t.Fatalf("setReflectValue() error = %v", err)
			}
			got := tt.args.v.Interface()
			if tt.args.v.Kind() == reflect.Pointer {
				got = tt.args.v.Elem().Interface()
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setReflectValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_setReflectValue_error(t *testing.T) {
	tests := []struct {
		name  string
		value any
		bs    []byte
	}{
		{"uint negative", new(uint), []byte("-1")},
		{"bool invalid", new(bool), []byte("maybe")},
		{"json invalid", new([]int), []byte("invalid")},
		{"time invalid", new(time.Time), []byte("invalid")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setReflectValue(reflect.ValueOf(tt.value).Elem(), tt.bs); err == nil {
				t.Fatalf("setReflectValue() error = nil")
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

type textValue string

func (v *textValue) UnmarshalText(text []byte) error {
	*v = textValue("decoded:" + string(text))
	return nil
}

var _ encoding.TextUnmarshaler = (*textValue)(nil)

type callFuncRecorder struct {
	called bool
}

func (r *callFuncRecorder) Mark() {
	r.called = true
}

func (r *callFuncRecorder) Fail() error {
	return errors.New("call failed")
}

func (r *callFuncRecorder) OK() error {
	r.called = true
	return nil
}

func TestSafeMapStringStringSetGet(t *testing.T) {
	safe := NewMapStringString()
	if got, ok := safe.Get("missing"); ok || got != "" {
		t.Fatalf("Get missing = %q, %v; want empty, false", got, ok)
	}

	safe.Set("answer", "42")
	if got, ok := safe.Get("answer"); !ok || got != "42" {
		t.Fatalf("Get answer = %q, %v; want 42, true", got, ok)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			safe.Set("shared", "ok")
			_, _ = safe.Get("shared")
		})
	}
	wg.Wait()
}

func TestNameConversionActualRules(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"HTTPServerUUID", "http_server_uuid"},
		{"URLValue", "url_value"},
		{"UserID", "user_id"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := ToDBName(tt.in); got != tt.want {
				t.Fatalf("ToDBName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}

	structTests := []struct {
		in   string
		want string
	}{
		{"rpc_url", "RPCURL"},
		{"user_id", "UserID"},
		{"", ""},
	}
	for _, tt := range structTests {
		t.Run(tt.in, func(t *testing.T) {
			if got := ToStructName(tt.in); got != tt.want {
				t.Fatalf("ToStructName(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestKsvsFormatsNormalExprAndAdd(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]any
		keyTail  []string
		wantKey  string
		wantArgs []any
	}{
		{
			name:     "normal",
			m:        map[string]any{"name": "alice"},
			keyTail:  []string{" = ? "},
			wantKey:  " `name` = ? ",
			wantArgs: []any{"alice"},
		},
		{
			name:     "expr",
			m:        map[string]any{"updated_at": NewExpr("NOW()")},
			wantKey:  "`updated_at` = NOW()",
			wantArgs: []any{},
		},
		{
			name:     "add",
			m:        map[string]any{"score": ExprIncrNum(5)},
			wantKey:  "`score` = `score` + ? ",
			wantArgs: []any{5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKeys, gotArgs := ksvs(tt.m, tt.keyTail...)
			if !reflect.DeepEqual(gotKeys, []string{tt.wantKey}) {
				t.Fatalf("keys = %#v, want %#v", gotKeys, []string{tt.wantKey})
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Fatalf("args = %#v, want %#v", gotArgs, tt.wantArgs)
			}
		})
	}
}

func TestArgsliceAndPlaceholderBoundaries(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, ""},
		{1, "?"},
		{4, "?,?,?,?"},
		{11, "?,?,?,?,?,?,?,?,?,?,?"},
	}
	for _, tt := range tests {
		t.Run(String(tt.n), func(t *testing.T) {
			if got := argslice(tt.n); got != tt.want {
				t.Fatalf("argslice(%d) = %q, want %q", tt.n, got, tt.want)
			}
			if got := Placeholder(tt.n); got != tt.want {
				t.Fatalf("Placeholder(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

func TestGetTagsAndStructToMap(t *testing.T) {
	type taggedRecord struct {
		ID        int
		Name      string
		Alias     string `mx:"nickname"`
		Ignored   string `mx:"-"`
		Missing   string
		EmptyDate string `mx:"empty_date"`
		Profile   struct {
			Age int
		} `mx:"profile"`
		Tags []string `mx:"tags"`
	}

	table := &Table{
		Columns: Columns{
			"id":         {Name: "id"},
			"name":       {Name: "name"},
			"nickname":   {Name: "nickname"},
			"empty_date": {Name: "empty_date", DataType: "date"},
			"profile":    {Name: "profile"},
			"tags":       {Name: "tags"},
		},
	}
	record := taggedRecord{
		Name:    "alice",
		Alias:   "ally",
		Ignored: "skip",
		Missing: "skip",
		Profile: struct {
			Age int
		}{Age: 7},
		Tags: []string{"red", "blue"},
	}
	v := reflect.ValueOf(record)

	gotTags := getTags(v, v.Type(), table)
	wantTags := []string{"id", "name", "nickname", "", "", "empty_date", "profile", "tags"}
	if !reflect.DeepEqual(gotTags, wantTags) {
		t.Fatalf("getTags() = %#v, want %#v", gotTags, wantTags)
	}

	got := structToMap(v, table)
	want := map[string]any{
		"name":     "alice",
		"nickname": "ally",
		"profile":  `{"Age":7}`,
		"tags":     `["red","blue"]`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("structToMap() = %#v, want %#v", got, want)
	}
}

func TestTimeHelpers(t *testing.T) {
	gotWhere := WhereTimeParse("created_at", "2026-05-13", 0, 0, 1)
	wantWhere := "(created_at >= '2026-05-13 00:00:00' AND created_at < '2026-05-14 00:00:00')"
	if gotWhere != wantWhere {
		t.Fatalf("WhereTimeParse() = %q, want %q", gotWhere, wantWhere)
	}

	gotTime, err := timeParse("2026-05-13 12:30")
	if err != nil {
		t.Fatalf("timeParse() error = %v", err)
	}
	wantTime := time.Date(2026, 5, 13, 12, 30, 0, 0, time.Local)
	if !gotTime.Equal(wantTime) {
		t.Fatalf("timeParse() = %v, want %v", gotTime, wantTime)
	}

	if _, err := timeParse("bad"); err == nil {
		t.Fatal("timeParse() error = nil, want error")
	}
}

func TestTimeParseModes(t *testing.T) {
	tests := []struct {
		name string
		in   Time
		want Time
	}{
		{
			name: "sday defaults eday",
			in:   Time{Sday: "2026-05-13"},
			want: Time{Sday: "2026-05-13", Eday: "2026-05-13", St: "2026-05-13 00:00:00", Et: "2026-05-13 23:59:59"},
		},
		{
			name: "day",
			in:   Time{Day: "2026-05-14"},
			want: Time{Day: "2026-05-14", St: "2026-05-14 00:00:00", Et: "2026-05-14 23:59:59"},
		},
		{
			name: "month",
			in:   Time{Month: "2026-02"},
			want: Time{Month: "2026-02", St: "2026-02-01 00:00:00", Et: "2026-02-28 23:59:59"},
		},
		{
			name: "smonth defaults emonth",
			in:   Time{Smonth: "2026-03"},
			want: Time{Smonth: "2026-03", Emonth: "2026-03", St: "2026-03-01 00:00:00", Et: "2026-03-31 23:59:59"},
		},
		{
			name: "stime defaults etime",
			in:   Time{St: "manual", Stime: "08:30"},
			want: Time{St: "manual", Stime: "08:30", Etime: "08:30"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in
			got.Parse()
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Parse() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestCallFunc(t *testing.T) {
	rec := &callFuncRecorder{}
	if err := callFunc(reflect.ValueOf(rec), "Mark"); err != nil {
		t.Fatalf("callFunc Mark error = %v", err)
	}
	if !rec.called {
		t.Fatal("callFunc Mark did not call method")
	}

	rec.called = false
	if err := callFunc(reflect.ValueOf(rec), "OK"); err != nil {
		t.Fatalf("callFunc OK error = %v", err)
	}
	if !rec.called {
		t.Fatal("callFunc OK did not call method")
	}

	if err := callFunc(reflect.ValueOf(rec), "Fail"); err == nil {
		t.Fatal("callFunc Fail error = nil, want error")
	}
	if err := callFunc(reflect.ValueOf(rec), "Missing"); err != nil {
		t.Fatalf("callFunc Missing error = %v, want nil", err)
	}
}

func TestParseReflectTimeLayouts(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Time
	}{
		{"empty", "", time.Time{}},
		{"zero mysql", "0000-00-00 00:00:00", time.Time{}},
		{"quoted", `"2026-05-13 12:30:45"`, time.Date(2026, 5, 13, 12, 30, 45, 0, time.Local)},
		{"month", "2026-05", time.Date(2026, 5, 1, 0, 0, 0, 0, time.Local)},
		{"year", "2026", time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)},
		{"rfc3339", "2026-05-13T12:30:45Z", time.Date(2026, 5, 13, 12, 30, 45, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseReflectTime([]byte(tt.in))
			if err != nil {
				t.Fatalf("parseReflectTime() error = %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("parseReflectTime() = %v, want %v", got, tt.want)
			}
		})
	}

	if _, err := parseReflectTime([]byte("not a time")); err == nil {
		t.Fatal("parseReflectTime() error = nil, want error")
	}
}

func TestSetReflectValueAdditionalTypes(t *testing.T) {
	type jsonStruct struct {
		A int `json:"a"`
	}

	var (
		text      textValue
		m         map[string]int
		st        jsonStruct
		iface     any
		reader    io.Reader
		emptyInt  = 9
		emptyBool = true
		emptyF64  = 3.14
		nullStr   = sql.NullString{String: "x", Valid: true}
		ptr       = new(int)
	)

	tests := []struct {
		name  string
		value reflect.Value
		in    []byte
		want  any
	}{
		{"text unmarshaler", reflect.ValueOf(&text).Elem(), []byte("abc"), textValue("decoded:abc")},
		{"map json", reflect.ValueOf(&m).Elem(), []byte(`{"a":1}`), map[string]int{"a": 1}},
		{"struct json", reflect.ValueOf(&st).Elem(), []byte(`{"a":2}`), jsonStruct{A: 2}},
		{"interface", reflect.ValueOf(&iface).Elem(), []byte("raw"), any("raw")},
		{"nil int", reflect.ValueOf(&emptyInt).Elem(), nil, 0},
		{"empty bool", reflect.ValueOf(&emptyBool).Elem(), []byte(""), false},
		{"empty float", reflect.ValueOf(&emptyF64).Elem(), []byte(""), float64(0)},
		{"nil scanner", reflect.ValueOf(&nullStr).Elem(), nil, sql.NullString{}},
		{"nil pointer", reflect.ValueOf(&ptr).Elem(), nil, (*int)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setReflectValue(tt.value, tt.in); err != nil {
				t.Fatalf("setReflectValue() error = %v", err)
			}
			if got := tt.value.Interface(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("setReflectValue() = %#v, want %#v", got, tt.want)
			}
		})
	}

	if err := setReflectValue(reflect.ValueOf(&reader).Elem(), []byte("raw")); err == nil {
		t.Fatal("setReflectValue() interface error = nil")
	}
	var complexValue complex64
	if err := setReflectValue(reflect.ValueOf(&complexValue).Elem(), []byte("1")); err == nil {
		t.Fatal("setReflectValue() complex error = nil")
	}
}

func TestExpandSliceReturnsCopyForAnySlice(t *testing.T) {
	in := []any{"a", "b"}
	got := expandSlice(in)
	got[0] = "changed"
	if in[0] != "a" {
		t.Fatalf("expandSlice mutated source slice: %#v", in)
	}
}

func TestGetFullSQL(t *testing.T) {
	got := GetFullSQL("SELECT * FROM t WHERE id = ? AND name = ? AND active = ?", 7, "alice", true)
	want := "SELECT * FROM t WHERE id = 7 AND name = 'alice' AND active = 'true'"
	if got != want {
		t.Fatalf("GetFullSQL() = %q, want %q", got, want)
	}
}

func TestKsvsMultipleKeysAsSet(t *testing.T) {
	gotKeys, gotArgs := ksvs(map[string]any{"a": 1, "b": 2}, " = ? ")
	sort.Strings(gotKeys)
	sort.Slice(gotArgs, func(i, j int) bool { return Int(gotArgs[i]) < Int(gotArgs[j]) })
	if !reflect.DeepEqual(gotKeys, []string{" `a` = ? ", " `b` = ? "}) {
		t.Fatalf("keys = %#v", gotKeys)
	}
	if !reflect.DeepEqual(gotArgs, []any{1, 2}) {
		t.Fatalf("args = %#v", gotArgs)
	}
}

func isSlice(args any) bool {
	return reflect.TypeOf(args).Kind() == reflect.Slice
}

func TestIsSlice(t *testing.T) {
	args := []any{}
	if !isSlice(args) {
		t.Fatal("fail with is slice")
	}
}

func JSONStringify(v any) string {
	bs, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(bs)
}
