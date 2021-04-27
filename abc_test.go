package mx

import (
	"database/sql"
	"fmt"
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
