package mx

import (
	"reflect"
)

const (
	C      = "CREATE"
	CREATE = C
	R      = "READ"
	READ   = R
	U      = "UPDATE"
	UPDATE = U
	D      = "DELETE"
	DELET  = D

	DBName = "DBName"

	BeforeCreate = "BeforeCreate"
	AfterCreate  = "AfterCreate"
	AfterFind    = "AfterFind"
	BeforeUpdate = "BeforeUpdate"
	AfterUpdate  = "AfterUpdate"
	BeforeDelete = "BeforeDelete"
	AfterDelete  = "AfterDelete"

	IsDeleted = "is_deleted"
)

type AfterFinder interface {
	AfterFind() error
}

// 获取结构体对应的数据库名
func getStructDBName(v reflect.Value) string {
	v = reflect.Indirect(v)
	var dbName string
	// 如果有DBName这个方法就调用这个获取表名，如果没有的话就通过toDBName获取表名
	dbNameFunc := v.MethodByName(DBName)
	if dbNameFunc.IsValid() {
		dbName = dbNameFunc.Call(nil)[0].String()
	} else {
		dbName = ToDBName(v.Type().Name())
	}
	return dbName
}

// 获取结构体ID
func getStructID(v reflect.Value) int64 {
	v = reflect.Indirect(v)
	rID := v.FieldByName("ID")
	if rID.IsValid() {
		return rID.Int()
	}
	return 0
}
