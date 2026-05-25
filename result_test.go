package mx

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type sqlResultStub struct {
	id   int64
	rows int64
}

func (r sqlResultStub) LastInsertId() (int64, error) { return r.id, nil }
func (r sqlResultStub) RowsAffected() (int64, error) { return r.rows, nil }

func openRowsMapStubDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(rowsMapTestDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func sampleRowsMap() RowsMap {
	return RowsMap{
		{"id": "1", "group": "a", "amount": "1.20", "qty": "2", "name": "bob", "flag": "1", "tags": `["red","blue"]`},
		{"id": "2", "group": "a", "amount": "2.35", "qty": "3", "name": "alice", "flag": "0"},
		{"id": "3", "group": "b", "amount": "8.70", "qty": "5", "name": "carol", "flag": "1"},
		{"id": "2", "group": "c", "amount": "0.10", "qty": "bad", "name": "alice2", "flag": "0"},
	}
}

func TestRowMapInterface_Int(t *testing.T) {
	type args struct {
		field string
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want int
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"field": "-5"}, args{field: "field"}, -5},
		{"", RowMapInterface{"field": -5}, args{field: "field"}, -5},
		{"", RowMapInterface{"field": "0"}, args{field: "field"}, 0},
		{"", RowMapInterface{"field": "123"}, args{field: "field"}, 123},
		{"", RowMapInterface{"field": "abc"}, args{field: "field"}, 0},
		{"", RowMapInterface{"field": "7.7"}, args{field: "field"}, 0},
		{"", RowMapInterface{"field": 7.7}, args{field: "field"}, 0},
		{"", RowMapInterface{"field": 7}, args{field: "field"}, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Int(tt.args.field); got != tt.want {
				t.Errorf("RowMapInterface.Int() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRowMapInterface_Ints(t *testing.T) {
	type args struct {
		field string
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want []int
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"ints": "[1,2,3]"}, args{field: "ints"}, []int{1, 2, 3}},
		{"", RowMapInterface{"ints": "[]"}, args{field: "ints"}, []int{}},
		{"", RowMapInterface{"ints": "1,2,3"}, args{field: "ints"}, []int{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Ints(tt.args.field); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowMapInterface.Ints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRowMapInterface_Strings(t *testing.T) {
	type args struct {
		field string
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want []string
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"ints": "[\"1\",\"2\",\"3\"]"}, args{field: "ints"}, []string{"1", "2", "3"}},
		{"", RowMapInterface{"ints": "[]"}, args{field: "ints"}, []string{}},
		{"", RowMapInterface{"ints": "\"1\",\"2\",\"3\""}, args{field: "ints"}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Strings(tt.args.field); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowMapInterface.Strings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRowMapInterface_Parse(t *testing.T) {
	type args struct {
		field string
		val   any
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want any
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"ints": "[1,2,3]"}, args{field: "ints", val: &[]int{}}, []int{1, 2, 3}},
		{"", RowMapInterface{"ints": "[\"1\",\"2\",\"3\"]"}, args{field: "ints", val: &[]string{}}, []string{"1", "2", "3"}},
		{"", RowMapInterface{"ints": `{"id":1,"name":"郭襄","lv":"出神莫化"}`}, args{field: "ints", val: &Weapon{}}, Weapon{ID: 1, Name: "郭襄", Lv: "出神莫化"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Parse(tt.args.field, tt.args.val); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowMapInterface.Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRowMapInterface_Interfaces(t *testing.T) {
	// 一开始以为数据对不上是因为cap不同， 后来发现cap相同，他们还是不一样。
	makeinteface := func(cap int, args ...any) []any {
		s := make([]any, 0, cap)
		s = append(s, args...)
		return s
	}
	type args struct {
		field string
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want []any
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"field": `[1,2,3]`}, args{field: "field"}, makeinteface(4, 1.0, 2.0, 3.0)},
		{"", RowMapInterface{"field": `["1","2","3"]`}, args{field: "field"}, []any{"1", "2", "3"}},
		{"", RowMapInterface{"field": `[1,"2",3]`}, args{field: "field"}, makeinteface(4, 1.0, "2", 3.0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Interfaces(tt.args.field); !reflect.DeepEqual(got, tt.want) {
				for idx := range got {
					t.Error(reflect.TypeOf(got[idx]), reflect.TypeOf(tt.want[idx]))
				}
				t.Errorf("RowMapInterface.Interfaces() = %v, want %v", got, tt.want)
				t.Error(len(got), cap(got), len(tt.want), cap(tt.want))
			}
		})
	}
}

func TestRowsMapInterface_Sort(t *testing.T) {
	type args struct {
		field  string
		isDesc []bool
		kind   reflect.Kind
	}
	tests := []struct {
		name string
		rs   RowsMapInterface
		args args
	}{
		// TODO: Add test cases.
		{
			"",
			RowsMapInterface{
				RowMapInterface{"val": 1},
				RowMapInterface{"val": 3},
				RowMapInterface{"val": 2},
			},
			args{field: "val", kind: reflect.Int},
		},
		{
			"",
			RowsMapInterface{
				RowMapInterface{"val": 1},
				RowMapInterface{"val": 3},
				RowMapInterface{"val": 2},
			},
			args{field: "val", isDesc: []bool{true}, kind: reflect.Int},
		},
		{
			"",
			RowsMapInterface{
				RowMapInterface{"val": "1"},
				RowMapInterface{"val": "3"},
				RowMapInterface{"val": "20"},
			},
			args{field: "val", kind: reflect.String},
		},
		{
			"",
			RowsMapInterface{
				RowMapInterface{"val": "1"},
				RowMapInterface{"val": "3"},
				RowMapInterface{"val": "20"},
			},
			args{field: "val", isDesc: []bool{true}, kind: reflect.String},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.rs.Sort(tt.args.field, tt.args.isDesc...)
			for i := 1; i < len(tt.rs); i++ {
				// 默认升序
				isGreater := false
				if tt.args.kind == reflect.Int {
					isGreater = tt.rs[i].Int(tt.args.field) >= tt.rs[i-1].Int(tt.args.field)
				} else {
					isGreater = tt.rs[i].String(tt.args.field) >= tt.rs[i-1].String(tt.args.field)
				}
				if len(tt.args.isDesc) > 0 {
					isGreater = !isGreater
				}
				if !isGreater {
					t.Fatal("顺序不对", tt.rs.Pluck(tt.args.field))
				}
			}
		})
	}
}

func TestSQLResultDelegatesAndReturnsStoredErrors(t *testing.T) {
	ok := &SQLResult{result: sqlResultStub{id: 9, rows: 3}}
	if got, err := ok.LastInsertId(); err != nil || got != 9 {
		t.Fatalf("LastInsertId() = %d, %v; want 9, nil", got, err)
	}
	if got, err := ok.RowsAffected(); err != nil || got != 3 {
		t.Fatalf("RowsAffected() = %d, %v; want 3, nil", got, err)
	}

	wantErr := errors.New("boom")
	failed := &SQLResult{err: wantErr}
	if got, err := failed.LastInsertId(); !errors.Is(err, wantErr) || got != 0 {
		t.Fatalf("LastInsertId() = %d, %v; want 0, %v", got, err, wantErr)
	}
	if got, err := failed.RowsAffected(); !errors.Is(err, wantErr) || got != 0 {
		t.Fatalf("RowsAffected() = %d, %v; want 0, %v", got, err, wantErr)
	}
}

func TestRowMapHelpers(t *testing.T) {
	row := RowMap{
		"id":      "7",
		"flag":    "1",
		"off":     "0",
		"bad":     "abc",
		"amount":  "2.50",
		"tags":    `["red","blue"]`,
		"payload": `{"name":"mx"}`,
	}

	if !row.Bool("flag") || row.Bool("off") || (RowMap{}).Bool("flag") {
		t.Fatalf("Bool helpers returned unexpected values")
	}
	if !(RowMap{"only": "1"}).Bool() || (RowMap{"only": "0"}).Bool() {
		t.Fatalf("Bool without field returned unexpected values")
	}
	if got := row.FieldDefault("missing", "def"); got != "def" {
		t.Fatalf("FieldDefault missing = %q, want def", got)
	}
	if got := row.FieldDefault("id", "def"); got != "7" {
		t.Fatalf("FieldDefault id = %q, want 7", got)
	}
	if !row.HaveRecord() || row.NotFound() || (RowMap{}).HaveRecord() || !(RowMap{}).NotFound() {
		t.Fatalf("HaveRecord/NotFound returned unexpected values")
	}
	if got := row.Int("id"); got != 7 {
		t.Fatalf("Int id = %d, want 7", got)
	}
	if got := row.Int("bad", 42); got != 42 {
		t.Fatalf("Int bad default = %d, want 42", got)
	}
	if got := row.Int32("id"); got != int32(7) {
		t.Fatalf("Int32 id = %d, want 7", got)
	}
	if got := row.Int32("bad", 9); got != int32(9) {
		t.Fatalf("Int32 bad default = %d, want 9", got)
	}
	if got := row.Incr("id"); got != 8 || row["id"] != "8" {
		t.Fatalf("Incr default = %d and row id %q, want 8", got, row["id"])
	}
	if got := row.Incr("id", 4); got != 12 || row["id"] != "12" {
		t.Fatalf("Incr custom = %d and row id %q, want 12", got, row["id"])
	}
	if got := row.Strings("tags"); !reflect.DeepEqual(got, []string{"red", "blue"}) {
		t.Fatalf("Strings() = %#v", got)
	}
	if got := row.Float64("amount"); got != 2.5 {
		t.Fatalf("Float64 amount = %v, want 2.5", got)
	}
	if got := row.Float64("bad", 1.25); got != 1.25 {
		t.Fatalf("Float64 bad default = %v, want 1.25", got)
	}

	var payload struct {
		Name string `json:"name"`
	}
	if err := row.Unmarshal("payload", &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Name != "mx" {
		t.Fatalf("payload.Name = %q, want mx", payload.Name)
	}

	copied := row.Copy()
	copied["id"] = "changed"
	if row["id"] == "changed" {
		t.Fatal("Copy() did not isolate map")
	}
	if got := row.Interface(); !reflect.DeepEqual(got["flag"], "1") {
		t.Fatalf("Interface() = %#v", got)
	}
}

func TestRowsMapAggregatesIndexesAndFilters(t *testing.T) {
	rows := sampleRowsMap()

	if got := rows.RowsMap(); !reflect.DeepEqual(got, rows) {
		t.Fatalf("RowsMap() = %#v, want original rows", got)
	}
	if got := rows.Sum("qty"); got != 10 {
		t.Fatalf("Sum(qty) = %d, want 10", got)
	}
	if got := rows.SumFloat("amount", 100); got != 1235 {
		t.Fatalf("SumFloat(amount) = %d, want 1235", got)
	}
	if got := rows.SumFloat64("amount", 100); got != 12.35 {
		t.Fatalf("SumFloat64(amount) = %v, want 12.35", got)
	}
	if got := rows.SumFloatString("amount"); got != "12.35" {
		t.Fatalf("SumFloatString(amount) = %q, want 12.35", got)
	}
	if got := rows.SumByFieldEq("qty", "group", "a"); got != 5 {
		t.Fatalf("SumByFieldEq() = %d, want 5", got)
	}
	if got := rows.Max("name"); got != "carol" {
		t.Fatalf("Max(name) = %q, want carol", got)
	}
	if got := rows.Len(); got != 4 {
		t.Fatalf("Len() = %d, want 4", got)
	}
	if got := rows.First(); !reflect.DeepEqual(got, rows[0]) {
		t.Fatalf("First() = %#v, want first row", got)
	}
	if got := (RowsMap{}).First(); !got.NotFound() {
		t.Fatalf("empty First() = %#v, want empty RowMap", got)
	}

	copied := rows.Copy()
	if len(copied) != len(rows) {
		t.Fatalf("Copy() length = %d, want %d", len(copied), len(rows))
	}
	copied[0]["name"] = "changed"
	if rows[0]["name"] == "changed" {
		t.Fatal("Copy() did not isolate nested rows")
	}
	if got := rows.String(); !reflect.DeepEqual(got[0], map[string]string(rows[0])) {
		t.Fatalf("String() = %#v", got)
	}
	if got := rows.Interface(); !reflect.DeepEqual(got[0]["name"], "bob") {
		t.Fatalf("Interface() = %#v", got)
	}

	if got := rows.MapIndex("id")["2"]["name"]; got != "alice2" {
		t.Fatalf("MapIndex duplicate id picked %q, want alice2", got)
	}
	if got := rows.MapIndexExist("group"); !got["a"] || !got["b"] || !got["c"] {
		t.Fatalf("MapIndexExist(group) = %#v", got)
	}
	if got := rows.MapIndexExistInt("id"); !got[1] || !got[2] || !got[3] {
		t.Fatalf("MapIndexExistInt(id) = %#v", got)
	}
	if got := rows.MapIndexIntExist("id"); !got[1] || !got[2] || !got[3] {
		t.Fatalf("MapIndexIntExist(id) = %#v", got)
	}
	if got := rows.MapIndexInt("id")[3]["name"]; got != "carol" {
		t.Fatalf("MapIndexInt(id)[3] = %q, want carol", got)
	}
	if got := rows.MapIndexKV("id", "name")["1"]; got != "bob" {
		t.Fatalf("MapIndexKV = %#v", got)
	}
	if got := rows.MapIndexIntKV("id", "name")[2]; got != "alice2" {
		t.Fatalf("MapIndexIntKV = %q, want alice2", got)
	}
	if got := rows.MapIndexIntString("id", "name")[3]; got != "carol" {
		t.Fatalf("MapIndexIntString = %q, want carol", got)
	}
	if got := rows.MapIndexStringInt("name", "qty")["alice"]; got != 3 {
		t.Fatalf("MapIndexStringInt = %d, want 3", got)
	}
	if got := rows.MapIndexIntKVInt("id", "qty")[2]; got != 0 {
		t.Fatalf("MapIndexIntKVInt duplicate id = %d, want 0 from bad qty", got)
	}
	if got := rows.MapIndexIntInt("id", "qty")[3]; got != 5 {
		t.Fatalf("MapIndexIntInt = %d, want 5", got)
	}
	if got := rows.MapIndexIntFloat("id", "amount")[1]; got != 1.20 {
		t.Fatalf("MapIndexIntFloat = %v, want 1.20", got)
	}
	if got := rows.MapIndexKVSum("group", "amount"); got["a"] != "3.55" || got["b"] != "8.7" || got["c"] != "0.1" {
		t.Fatalf("MapIndexKVSum = %#v", got)
	}
	if got := rows.MapIndexs("group"); len(got["a"]) != 2 || len(got["b"]) != 1 {
		t.Fatalf("MapIndexs(group) = %#v", got)
	}
	if got := rows.MapIndexsInt("id"); len(got[2]) != 2 || len(got[1]) != 1 {
		t.Fatalf("MapIndexsInt(id) = %#v", got)
	}
	if !(Vals{"red", "blue"}).Contains("blue") || (Vals{"red"}).Contains("blue") {
		t.Fatalf("Vals.Contains returned unexpected values")
	}
	if got := rows.MapIndexsKV("group", "name"); !reflect.DeepEqual(got["a"], Vals{"bob", "alice"}) {
		t.Fatalf("MapIndexsKV = %#v", got)
	}

	if got := rows.Filter("group", "a"); len(got) != 2 {
		t.Fatalf("Filter(group=a) length = %d, want 2", len(got))
	}
	if got := rows.FilterContains([]string{"name"}, "ali"); len(got) != 2 {
		t.Fatalf("FilterContains(name, ali) length = %d, want 2", len(got))
	}
	if got := rows.FilterContains([]string{"name"}, ""); !reflect.DeepEqual(got, rows) {
		t.Fatalf("FilterContains empty = %#v, want original rows", got)
	}
	if got := rows.FilterIn("id", []string{"1", "3"}); len(got) != 2 {
		t.Fatalf("FilterIn length = %d, want 2", len(got))
	}
	if got := rows.FilterNotIn("id", []string{"1", "3"}); len(got) != 2 {
		t.Fatalf("FilterNotIn length = %d, want 2", len(got))
	}
	if got := rows.FilterFunc(func(r RowMap) bool { return r.Bool("flag") }); len(got) != 2 {
		t.Fatalf("FilterFunc flag length = %d, want 2", len(got))
	}
}

func TestRowsMapMutationGroupingLookupAndSorting(t *testing.T) {
	rows := sampleRowsMap()

	rows.EachMod(func(r RowMap) {
		r["seen"] = "1"
	})
	for _, row := range rows {
		if row["seen"] != "1" {
			t.Fatalf("EachMod did not update row %#v", row)
		}
	}

	grouped := rows.GroupByField("group")
	if len(grouped) != 3 || grouped[0].Key != "a" || grouped[0].Len != 2 {
		t.Fatalf("GroupByField() = %#v", grouped)
	}
	sqlGrouped := rows.GroupBySQL("group", "amount", "qty")
	groupIndex := sqlGrouped.MapIndex("group")
	if groupIndex["a"]["amount"] != "3.55" || groupIndex["a"]["qty"] != "5" {
		t.Fatalf("GroupBySQL group a = %#v", groupIndex["a"])
	}

	if !rows.HaveID("2") || rows.HaveID("9") {
		t.Fatalf("HaveID returned unexpected values")
	}
	if got := rows.RowID("3"); got["name"] != "carol" {
		t.Fatalf("RowID(3) = %#v", got)
	}
	if got := rows.RowID("9"); got != nil {
		t.Fatalf("RowID(9) = %#v, want nil", got)
	}
	if got := rows.RowField("alice", "name"); got["id"] != "2" {
		t.Fatalf("RowField(alice,name) = %#v", got)
	}
	if got := rows.RowField("missing", "name"); !got.NotFound() {
		t.Fatalf("RowField missing = %#v, want empty", got)
	}
	if got := rows.RowsField("2", "id"); len(got) != 2 {
		t.Fatalf("RowsField id=2 length = %d, want 2", len(got))
	}
	if got := rows.Pluck("id"); !reflect.DeepEqual(got, []any{"1", "2", "3", "2"}) {
		t.Fatalf("Pluck(id) = %#v", got)
	}
	if got := rows.PluckString("name"); !reflect.DeepEqual(got, []string{"bob", "alice", "carol", "alice2"}) {
		t.Fatalf("PluckString(name) = %#v", got)
	}
	if got := rows.PluckInt("id"); !reflect.DeepEqual(got, []int{1, 2, 3, 2}) {
		t.Fatalf("PluckInt(id) = %#v", got)
	}
	if got := rows.Unique("id"); len(got) != 3 || got[1]["id"] != "2" {
		t.Fatalf("Unique(id) = %#v", got)
	}

	rows.CoverFieldByField("copied_name", "name")
	if rows[0]["copied_name"] != "bob" {
		t.Fatalf("CoverFieldByField did not copy name")
	}

	byName := rows.Copy()
	byName.Sort("name", false)
	if got := byName.PluckString("name"); !reflect.DeepEqual(got, []string{"alice", "alice2", "bob", "carol"}) {
		t.Fatalf("Sort name asc = %#v", got)
	}
	byIDDesc := rows.Copy()
	byIDDesc.SortInt("id", true)
	if got := byIDDesc.PluckString("id"); !reflect.DeepEqual(got, []string{"3", "2", "2", "1"}) {
		t.Fatalf("SortInt id desc = %#v", got)
	}
	byAmount := rows.Copy()
	byAmount.SortFloat("amount", false)
	if got := byAmount.PluckString("amount"); !reflect.DeepEqual(got, []string{"0.10", "1.20", "2.35", "8.70"}) {
		t.Fatalf("SortFloat amount asc = %#v", got)
	}
	byCustom := rows.Copy()
	byCustom.SortFunc(func(rm RowsMap, i, j int) bool {
		return len(rm[i]["name"]) < len(rm[j]["name"])
	})
	if byCustom[0]["name"] != "bob" {
		t.Fatalf("SortFunc shortest name first = %#v", byCustom.PluckString("name"))
	}
}

func TestRowsWrapsAndMultiWarpByField(t *testing.T) {
	rows := RowsMap{
		{"id": "1", "group": "a", "name": "one"},
		{"id": "2", "group": "a", "name": "two"},
		{"id": "3", "group": "b", "name": "three"},
	}

	wraps := rows.WarpByField("group")
	if !wraps.HaveKey("a") || wraps.HaveKey("missing") || len(wraps[0].Val) != 2 {
		t.Fatalf("WarpByField() = %#v", wraps)
	}
	wraps.Set("empty", nil)
	if !wraps.HaveKey("empty") {
		t.Fatalf("Set nil did not add empty key: %#v", wraps)
	}
	wraps.Sort(func(rw *RowsWraps, i, j int) bool {
		return (*rw)[i].Key > (*rw)[j].Key
	})
	if wraps[0].Key < wraps[len(wraps)-1].Key {
		t.Fatalf("Sort desc failed: %#v", wraps)
	}

	treeRows := RowsMap{
		{"province_id": "1", "province_name": "P1", "city_id": "10", "city_name": "C1", "district_id": "100", "district_name": "D1"},
		{"province_id": "1", "province_name": "P1", "city_id": "10", "city_name": "C1", "district_id": "101", "district_name": "D2"},
		{"province_id": "1", "province_name": "P1", "city_id": "11", "city_name": "C2", "district_id": "110", "district_name": "D3"},
		{"province_id": "2", "province_name": "P2", "city_id": "20", "city_name": "C3", "district_id": "", "district_name": ""},
	}
	tree := treeRows.MultiWarpByField("province_id", "province_name", "city_id", "city_name", "district_id", "district_name")
	if len(tree) != 2 {
		t.Fatalf("MultiWarpByField top len = %d, want 2: %#v", len(tree), tree)
	}
	if tree[0].ID != "1" || tree[0].Name != "P1" || len(tree[0].Vals) != 2 {
		t.Fatalf("MultiWarpByField first node = %#v", tree[0])
	}
	if tree[0].Vals[0].ID != "10" || len(tree[0].Vals[0].Vals) != 2 {
		t.Fatalf("MultiWarpByField first city = %#v", tree[0].Vals[0])
	}
	if got := treeRows.MultiWarpByField("province_id"); len(got) != 0 {
		t.Fatalf("MultiWarpByField invalid fields = %#v, want empty", got)
	}
}

func TestRowMapInterfaceMoreHelpers(t *testing.T) {
	row := RowMapInterface{
		"id":      7,
		"name":    "alice",
		"payload": `{"ok":true}`,
	}

	if got := row.String("id"); got != "7" {
		t.Fatalf("String(id) = %q, want 7", got)
	}
	if got := string(row.Bytes("name")); got != "alice" {
		t.Fatalf("Bytes(name) = %q, want alice", got)
	}
	if got := row.RowMap(); !reflect.DeepEqual(got, RowMap{"id": "7", "name": "alice", "payload": `{"ok":true}`}) {
		t.Fatalf("RowMap() = %#v", got)
	}
	var parsed struct {
		OK bool `json:"ok"`
	}
	if got := row.Parse("payload", &parsed); !reflect.DeepEqual(got, struct {
		OK bool `json:"ok"`
	}{OK: true}) {
		t.Fatalf("Parse(payload) = %#v", got)
	}
	if got := row.Parse("payload", &[]int{}); got != nil {
		t.Fatalf("Parse invalid = %#v, want nil", got)
	}

	rows := RowsMapInterface{
		{"id": 2, "name": "b"},
		{"id": 1, "name": "a"},
		{"id": 3, "name": "a"},
	}
	if got := rows.Pluck("name"); !reflect.DeepEqual(got, []any{"b", "a", "a"}) {
		t.Fatalf("Pluck(name) = %#v", got)
	}
	if got := rows.Filter("name", "a"); len(got) != 2 {
		t.Fatalf("Filter(name=a) length = %d, want 2", len(got))
	}
	if got := rows.FilterFunc(func(r RowMapInterface) bool { return r.Int("id") > 1 }); len(got) != 2 {
		t.Fatalf("FilterFunc id>1 length = %d, want 2", len(got))
	}
	RowsMapInterface{}.Sort("id")
	rows.Sort("id")
	if got := rows.Pluck("id"); !reflect.DeepEqual(got, []any{1, 2, 3}) {
		t.Fatalf("Sort id asc = %#v", got)
	}
	rows.Sort("name", true)
	if got := rows[0].String("name"); got != "b" {
		t.Fatalf("Sort name desc first = %q, want b", got)
	}
}

func TestSQLRowsMoreHelpers(t *testing.T) {
	db := openRowsMapStubDB(t)
	defer db.Close()

	queryRows := func(t *testing.T) *SQLRows {
		t.Helper()
		rows, err := db.Query("SELECT * FROM stub")
		if err != nil {
			t.Fatal(err)
		}
		return &SQLRows{rows: rows}
	}
	querySingle := func(t *testing.T) *SQLRows {
		t.Helper()
		rows, err := db.Query("SELECT single FROM stub")
		if err != nil {
			t.Fatal(err)
		}
		return &SQLRows{rows: rows}
	}

	if got := (&SQLRows{err: errors.New("err")}).Pluck("id"); len(got) != 0 {
		t.Fatalf("Pluck on error = %#v, want empty", got)
	}
	if got := queryRows(t).Pluck("name"); !reflect.DeepEqual(got, []any{"alice", "bob"}) {
		t.Fatalf("Pluck(name) = %#v", got)
	}
	if got := queryRows(t).PluckInt("id"); !reflect.DeepEqual(got, []int{7, 0}) {
		t.Fatalf("PluckInt(id) = %#v", got)
	}
	if got := queryRows(t).PluckString("id"); !reflect.DeepEqual(got, []string{"7", ""}) {
		t.Fatalf("PluckString(id) = %#v", got)
	}
	if got := queryRows(t).RowMapInterface(); !reflect.DeepEqual(got, RowMapInterface{"id": "7", "name": "alice"}) {
		t.Fatalf("RowMapInterface() = %#v", got)
	}
	if got := (&SQLRows{err: errors.New("err")}).RowMapInterface(); len(got) != 0 {
		t.Fatalf("RowMapInterface error = %#v, want empty", got)
	}
	if got := queryRows(t).RowMap(); !reflect.DeepEqual(got, RowMap{"id": "7", "name": "alice"}) {
		t.Fatalf("RowMap() = %#v", got)
	}

	colIndex, data := queryRows(t).DoubleSlice()
	if colIndex["id"] != 0 || colIndex["name"] != 1 || !reflect.DeepEqual(data, [][]string{{"7", "alice"}, {"", "bob"}}) {
		t.Fatalf("DoubleSlice() = %#v, %#v", colIndex, data)
	}
	byteIndex, byteData := queryRows(t).TripleByte()
	if byteIndex["id"] != 0 || byteIndex["name"] != 1 || string(byteData[0][0]) != "7" || string(byteData[1][1]) != "bob" {
		t.Fatalf("TripleByte() = %#v, %#v", byteIndex, byteData)
	}
	if got := querySingle(t).Int(); got != 7 {
		t.Fatalf("Int() = %d, want 7", got)
	}
	if got := querySingle(t).String(); got != "7" {
		t.Fatalf("String() = %q, want 7", got)
	}
	var scanned string
	if err := querySingle(t).Scan(&scanned); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if scanned != "7" {
		t.Fatalf("Scan() scanned %q, want 7", scanned)
	}
	if err := (&SQLRows{err: driver.ErrBadConn}).Scan(&scanned); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Scan error = %v, want ErrBadConn", err)
	}
}

func TestSQLRowsFindAndToStruct(t *testing.T) {
	db := openRowsMapStubDB(t)
	defer db.Close()

	queryRows := func(t *testing.T) *SQLRows {
		t.Helper()
		rows, err := db.Query("SELECT * FROM stub")
		if err != nil {
			t.Fatal(err)
		}
		return &SQLRows{rows: rows}
	}

	type rowStruct struct {
		ID   int
		Name string
	}

	var one rowStruct
	if err := queryRows(t).ToStruct(&one); err != nil {
		t.Fatalf("ToStruct struct error = %v", err)
	}
	if !reflect.DeepEqual(one, rowStruct{ID: 7, Name: "alice"}) {
		t.Fatalf("ToStruct struct = %#v", one)
	}

	var many []rowStruct
	if err := queryRows(t).ToStruct(&many); err != nil {
		t.Fatalf("ToStruct slice error = %v", err)
	}
	if !reflect.DeepEqual(many, []rowStruct{{ID: 7, Name: "alice"}, {ID: 0, Name: "bob"}}) {
		t.Fatalf("ToStruct slice = %#v", many)
	}

	var found rowStruct
	if err := queryRows(t).Find(&found); err != nil {
		t.Fatalf("Find struct error = %v", err)
	}
	if !reflect.DeepEqual(found, rowStruct{ID: 7, Name: "alice"}) {
		t.Fatalf("Find struct = %#v", found)
	}

	var foundMany []rowStruct
	if err := queryRows(t).Find(&foundMany); err != nil {
		t.Fatalf("Find slice error = %v", err)
	}
	if !reflect.DeepEqual(foundMany, []rowStruct{{ID: 7, Name: "alice"}, {ID: 0, Name: "bob"}}) {
		t.Fatalf("Find slice = %#v", foundMany)
	}

	var foundInt int
	if err := queryRows(t).Find(&foundInt); err != nil {
		t.Fatalf("Find int error = %v", err)
	}
	if foundInt != 7 {
		t.Fatalf("Find int = %d, want 7", foundInt)
	}

	var foundString string
	if err := queryRows(t).Find(&foundString); err != nil {
		t.Fatalf("Find string error = %v", err)
	}
	if foundString != "alice" && foundString != "7" {
		t.Fatalf("Find string = %q, want one scanned field", foundString)
	}

	if err := queryRows(t).ToStruct(new(int)); err == nil || !strings.Contains(err.Error(), "Unsupport Type") {
		t.Fatalf("ToStruct unsupported error = %v", err)
	}
}
