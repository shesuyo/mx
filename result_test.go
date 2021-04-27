package mx

import (
	"reflect"
	"testing"
)

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
		val   interface{}
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want interface{}
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
	makeinteface := func(cap int, args ...interface{}) []interface{} {
		s := make([]interface{}, 0, cap)
		for _, arg := range args {
			s = append(s, arg)
		}
		return s
	}
	type args struct {
		field string
	}
	tests := []struct {
		name string
		rm   RowMapInterface
		args args
		want []interface{}
	}{
		// TODO: Add test cases.
		{"", RowMapInterface{"field": `[1,2,3]`}, args{field: "field"}, makeinteface(4, 1, 2, 3)},
		{"", RowMapInterface{"field": `["1","2","3"]`}, args{field: "field"}, []interface{}{"1", "2", "3"}},
		{"", RowMapInterface{"field": `[1,"2",3]`}, args{field: "field"}, makeinteface(4, 1, "2", 3)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rm.Interfaces(tt.args.field); !reflect.DeepEqual(got, tt.want) {
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
