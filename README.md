# mx

`mx` 是一个轻量级、SQL 优先的 Go ORM。它主要面向 MySQL，提供表级链式查询、map CRUD、结构体映射、查询结果转换和常用结果集处理能力。

这个库不试图替代 SQL。简单查询可以用链式 API 提高效率；复杂 SQL、复杂 JOIN、窗口函数、事务和厂商特性，建议直接使用 `db.Query` / `db.Exec` 写 SQL。

## 特性

- 基于 `database/sql`，默认使用 `github.com/go-sql-driver/mysql`
- 表级链式查询：`Where`、`In`、`MustIn`、`Joins`、`Fields`、`Group`、`Having`、`OrderBy`、`Limit`、`Page`
- map CRUD：`Create`、`Creates`、`Update`、`Delete`、`Read`、`Reads`、`CreateOrUpdate`
- 结构体 CRUD：`Create`、`Update`、`Delete`、`Save`、`Struct`
- 查询结果支持 `RowsMap`、`RowMap`、`RowsMapInterface`、`RowsMapNull`、结构体扫描
- 支持 `BeforeCreate`、`AfterCreate`、`BeforeUpdate`、`AfterUpdate`、`BeforeDelete`、`AfterDelete`、`AfterFind` 钩子
- 自动读取 MySQL `information_schema.COLUMNS`，用于字段识别、软删除识别、结构体映射和自动 JOIN
- 表存在 `is_deleted` 字段时，查询自动追加 `is_deleted = 0`，删除会转为软删除

## 安装

```bash
go get github.com/shesuyo/mx
```

## 快速开始

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/shesuyo/mx"
)

func main() {
	db, err := mx.NewDataBase("user:password@tcp(127.0.0.1:3306)/demo?charset=utf8mb4&parseTime=true", mx.Config{
		MaxIdleConns: 10,
		MaxOpenConns: 50,
		Timeout:      3 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	users := db.Table("user").
		Fields("id", "name", "age").
		Where("age > ?", 18).
		OrderBy("id", true).
		Limit(10).
		RowsMap()

	fmt.Println(users)
}
```

开启调试 SQL：

```go
db.Debug(true)

rows := db.Table("user").
	Where("age > ?", 18).
	RowsMap()
```

也可以直接写 SQL：

```go
rows := db.Query(
	"SELECT id, name FROM user WHERE age > ? ORDER BY id DESC LIMIT ?",
	18,
	10,
).RowsMap()
```

## 查询构造器

基础查询：

```go
rows := db.Table("user").
	Fields("id", "name", "age").
	Where("age >= ?", 18).
	WhereLike("name", "alice").
	OrderBy("created_at", true).
	Page(20, 1).
	RowsMap()
```

常用条件：

```go
t := db.Table("order")

rows := t.
	WhereNotEmpty("status = ?", status).
	WhereTime("created_at", mx.Time{Sday: "2026-01-01", Eday: "2026-01-31"}).
	In("user_id", 1, 2, 3).
	RowsMap()
```

`In` 和 `MustIn` 的区别很重要：

```go
// ids 为空时，In 不追加任何条件，可能查出更多数据。
rows := db.Table("user").In("id", ids).RowsMap()

// ids 为空时，MustIn 直接返回空结果，不执行查询。
rows = db.Table("user").MustIn("id", ids).RowsMap()
```

JOIN：

```go
rows := db.Table("user").
	Joins("weapon").
	Fields("user.id", "user.name", "weapon.name AS weapon_name").
	RowsMap()
```

如果自动 JOIN 规则不满足，可以传入明确条件：

```go
rows := db.Table("user").
	Joins("weapon", "weapon.owner_id = user.id").
	Fields("user.id", "weapon.name AS weapon_name").
	RowsMap()
```

聚合：

```go
rows := db.Table("user").
	Fields("age", "COUNT(*) AS total").
	Group("age").
	Having("COUNT(*) > ?", 1).
	RowsMap()
```

查看生成的 SQL：

```go
query, args := db.Table("user").
	Fields("id", "name").
	Where("age > ?", 18).
	Limit(10).
	Parse()
```

## map CRUD

创建：

```go
id, err := db.Table("user").Create(map[string]any{
	"name": "alice",
	"age":  20,
})
```

批量创建：

```go
affected, err := db.Table("user").Creates([]map[string]any{
	{"name": "alice", "age": 20},
	{"name": "bob", "age": 21},
})
```

更新：

```go
affected, err := db.Table("user").Update(map[string]any{
	"id":   id,
	"name": "Alice",
})
```

自增表达式：

```go
affected, err := db.Table("counter").Update(map[string]any{
	"id":  1,
	"num": mx.ExprIncrNum(3),
})
```

删除：

```go
affected, err := db.Table("user").Delete(map[string]any{"id": id})
```

唯一字段检查：

```go
id, err := db.Table("user").Create(map[string]any{
	"email": "alice@example.com",
	"name":  "alice",
}, "email")
if err == mx.ErrInsertRepeat {
	// email 已存在
}
```

创建或更新：

```go
err := db.Table("user").CreateOrUpdate(map[string]any{
	"email": "alice@example.com",
	"name":  "Alice",
}, "email")
```

## 结构体映射

默认表名和字段名会按 Go 名称转换为 snake_case：

```go
type UserProfile struct {
	ID     int    `json:"id"`
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
}

// 默认表名：user_profile
// 默认字段：id, user_id, name
```

自定义表名：

```go
func (UserProfile) DBName() string {
	return "member_profile"
}
```

字段标签：

```go
type User struct {
	ID       int    `json:"id"`
	Name     string `mx:"nickname"`
	Password string `mx:"-"`
}
```

- `mx:"column_name"` 指定数据库字段名
- `mx:"-"` 忽略字段
- 未设置 `mx` 时，扫描路径会优先使用 `json` 标签，再回退到字段名的 snake_case

结构体 CRUD：

```go
u := User{Name: "alice"}

id, err := db.Create(&u)
if err != nil {
	return err
}

u.Name = "Alice"
err = db.Update(&u)

_, err = db.Delete(&u)
```

保存：

```go
rsp, err := db.Table("user").Save(&u, mx.OmitFields{"temporary_field"})
```

`Save` 会根据结构体的 `ID` 是否为 0 自动选择创建或更新。

查询到结构体：

```go
var users []User
err := db.Table("user").
	Where("age > ?", 18).
	Struct(&users)

var one User
err = db.Table("user").
	WhereID(1).
	Limit(1).
	Struct(&one)
```

## 钩子

结构体可以实现这些方法：

```go
func (u *User) BeforeCreate() error { return nil }
func (u *User) AfterCreate() error  { return nil }
func (u *User) BeforeUpdate() error { return nil }
func (u *User) AfterUpdate() error  { return nil }
func (u *User) BeforeDelete() error { return nil }
func (u *User) AfterDelete() error  { return nil }
func (u *User) AfterFind() error    { return nil }
```

不同调用路径对钩子错误的处理略有差异。业务逻辑依赖钩子错误时，建议补充测试锁定行为。

## 查询结果处理

`RowsMap` / `RowMap` 默认把数据库值转成字符串：

```go
row := db.Table("user").WhereID(1).RowMap()
name := row["name"]
age := row.Int("age")

rows := db.Table("user").RowsMap()
ids := rows.PluckInt("id")
byID := rows.MapIndexInt("id")
byStatus := rows.MapIndexs("status")
```

常用方法：

```go
row.HaveRecord()
row.NotFound()
row.Int("id")
row.Float64("amount")
row.Bool("enabled")
row.Unmarshal("json_field", &dst)

rows.First()
rows.Len()
rows.Sum("amount")
rows.Filter("status", "ok")
rows.FilterIn("id", []string{"1", "2"})
rows.Unique("user_id")
rows.SortInt("id", true)
```

需要保留 SQL `NULL` 时使用：

```go
rows := db.Table("user").RowsMapNull()
```

需要更低分配的结果时使用：

```go
cols, data := db.Table("user").Fields("id", "name").DoubleSlice()
_ = cols
_ = data
```

## 软删除

如果表中存在 `is_deleted` 字段：

- 查询构造器会自动追加 `is_deleted = 0`
- `Table.Delete` 会执行 `UPDATE table SET is_deleted = '1', deleted_at = ...`
- `Reads` 会向传入的条件 map 中追加 `is_deleted: 0`

因此有软删除字段的表，删除不是物理删除。

## 注意事项

- `Table.Update` 默认使用 `id` 作为条件，并且总是追加 `LIMIT 1`
- `Table.Delete` 不会自动追加 `LIMIT 1`，会删除或软删除所有匹配条件的数据
- `Table.Delete(map[string]any{})` 会返回 `ErrNoDeleteKey`
- `In` / `NotIn` 传入空数组时不会追加条件；空数组应返回空结果时请使用 `MustIn`
- 结构体 CRUD 和结构体扫描必须传指针，例如 `db.Create(&u)`、`table.Struct(&users)`
- `RowsMap` 和大部分 `RowsMapInterface` 结果值是字符串；需要 `NULL` 语义时使用 `RowsMapNull`
- `GetFullSQL` 只适合调试日志，不是 SQL 转义函数，执行 SQL 时仍应使用参数绑定
- 复杂 SQL 建议直接写 `db.Query` / `db.Exec`，不要为了使用 ORM 强行拼链式调用

## 其他数据库驱动

`NewDataBase` 中保留了部分驱动前缀识别：

- `postgres://`
- `dm://`
- `db2://`
- `sqlserver://`

当前模块只直接引入 MySQL 驱动。使用其他数据库时，应用侧需要自行引入对应 `database/sql` 驱动，并验证字段元数据、SQL 方言和扫描行为。

## 操作系统支持

64 位操作系统。
