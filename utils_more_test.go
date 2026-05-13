package mx

import (
	"database/sql"
	"encoding"
	"errors"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

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
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			safe.Set("shared", "ok")
			_, _ = safe.Get("shared")
		}()
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
