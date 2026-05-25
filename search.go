package mx

import (
	"fmt"
	"strconv"
	"strings"
)

// JoinCon join条件
type JoinCon struct {
	TableName string
	Condition string
}

// JoinCons join条件slice
type JoinCons []JoinCon

// HaveTable join条件中是否已经添加了这张表的join
func (jc JoinCons) HaveTable(tableName string) bool {
	for _, v := range jc {
		if v.TableName == tableName {
			return true
		}
	}
	return false
}

// WhereCon where条件
type WhereCon struct {
	Query string
	Args  []any
}

type exprSpec uint8

const (
	ExprNormal exprSpec = iota
	ExprAdd
)

// Expr 表达式
type Expr struct {
	State string
	Args  []any
	spec  exprSpec
}

var (
	ExprIncr = Expr{Args: []any{1}, spec: ExprAdd} // field + ? , 1
)

func ExprIncrNum(num any) Expr {
	return Expr{Args: []any{num}, spec: ExprAdd}
}

func NewExpr(state string, args ...any) Expr {
	return Expr{
		State: state,
		Args:  args,
	}
}

// Search 搜索结构体
type Search struct {
	table *Table

	fields            []string
	tableName         string
	joinConditions    JoinCons
	whereConditions   []WhereCon
	orderbyConditions []string
	groupConditions   []string
	havingConditions  []WhereCon
	with              string
	having            string
	limit             any
	offset            any

	query       string
	args        []any
	raw         bool
	debug       bool
	noNeedQuery bool
}

// Clone 克隆当前 Search，返回独立的浅拷贝实例。
func (s *Search) Clone() *Search {
	clone := *s
	return &clone
}

// Fields 添加需要查询的字段，支持 $C 或 $c 快捷写法表示 COUNT(*) AS total。
func (s *Search) Fields(args ...string) *Search {
	if len(args) == 0 {
		return s
	}
	for i := range args {
		switch args[i] {
		case "$C", "$c":
			args[i] = "COUNT(*) AS total"
		}
	}
	s.fields = append(s.fields, args...)
	return s
}

// Where 添加自定义 WHERE 条件和对应参数。
func (s *Search) Where(query string, values ...any) *Search {
	s.whereConditions = append(s.whereConditions, WhereCon{Query: query, Args: values})
	return s
}

// WhereID 添加当前表 id 等于指定值的 WHERE 条件。
func (s *Search) WhereID(id any) *Search {
	s.whereConditions = append(s.whereConditions, WhereCon{Query: s.tableName + ".id = ?", Args: []any{id}})
	return s
}

// In 添加 field IN (...) 条件，args 可传多个值或单个切片；空参数时不添加条件。
func (s *Search) In(field string, args ...any) *Search {
	//in没有参数的话SQL就会报错
	if len(args) == 0 {
		return s
	}
	if len(args) == 1 {
		args = expandSlice(args[0])
	}
	// 解析之后还可能为0
	if len(args) == 0 {
		return s
	}
	s.whereConditions = append(s.whereConditions, WhereCon{Query: fmt.Sprintf("%s IN (%s)", field, placeholder(len(args))), Args: args})
	return s
}

// MustIn 添加 field IN (...) 条件，空参数或空切片时标记为无需查询。
func (s *Search) MustIn(field string, args ...any) *Search {
	// 强制 in nothing 不查询。
	if len(args) == 0 {
		s.noNeedQuery = true
		return s
	}
	if len(args) == 1 {
		args = expandSlice(args[0])
	}
	// 解析之后还可能为0
	if len(args) == 0 {
		s.noNeedQuery = true
		return s
	}
	s.whereConditions = append(s.whereConditions, WhereCon{Query: fmt.Sprintf("%s IN (%s)", field, placeholder(len(args))), Args: args})
	return s
}

// NotIn 添加 field NOT IN (...) 条件，args 可传多个值或单个切片；空参数时不添加条件。
func (s *Search) NotIn(field string, args ...any) *Search {
	if len(args) == 0 {
		return s
	}
	if len(args) == 1 {
		args = expandSlice(args[0])
	}
	if len(args) == 0 {
		return s
	}
	s.whereConditions = append(s.whereConditions, WhereCon{Query: fmt.Sprintf("%s NOT IN (%s)", field, placeholder(len(args))), Args: args})
	return s
}

// Joins 添加 LEFT JOIN 条件；未指定 condition 时按表字段关系自动生成关联条件。
func (s *Search) Joins(tablename string, condition ...string) *Search {
	if len(condition) == 1 {
		s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: condition[0]})
	} else {
		if s.table.tableColumns[tablename].HaveColumn(s.tableName + "id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"id", s.tableName)})
		} else if s.table.tableColumns[tablename].HaveColumn(s.tableName + "_id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", tablename, s.tableName+"_id", s.tableName)})
		} else if s.table.tableColumns[s.tableName].HaveColumn(tablename + "id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"id", tablename)})
		} else if s.table.tableColumns[s.tableName].HaveColumn(tablename + "_id") {
			s.joinConditions = append(s.joinConditions, JoinCon{TableName: tablename, Condition: fmt.Sprintf("%s.%s = %s.id", s.tableName, tablename+"_id", tablename)})
		}
	}
	return s
}

// OrderBy 添加排序条件，默认升序；isDESC 为 true 时使用降序。
func (s *Search) OrderBy(field string, isDESC ...bool) *Search {
	if len(isDESC) > 0 && isDESC[0] {
		s.orderbyConditions = append(s.orderbyConditions, field+" DESC")
	} else {
		s.orderbyConditions = append(s.orderbyConditions, field+" ASC")
	}
	return s
}

// TableName 设置当前查询使用的主表名。
func (s *Search) TableName(name string) *Search {
	s.tableName = name
	return s
}

// Limit 设置 SELECT 语句的 LIMIT 参数。
func (s *Search) Limit(limit any) *Search {
	s.limit = limit
	return s
}

// Offset 设置 SELECT 语句的 OFFSET 参数。
func (s *Search) Offset(offset any) *Search {
	s.offset = offset
	return s
}

// Group 添加 GROUP BY 字段。
func (s *Search) Group(field ...string) *Search {
	s.groupConditions = append(s.groupConditions, field...)
	return s
}

// Having 添加 HAVING 条件和对应参数。
func (s *Search) Having(query string, args ...any) *Search {
	s.havingConditions = append(s.havingConditions, WhereCon{Query: query, Args: args})
	return s
}

// Parse 将 Search 中的字段、关联、条件、分组、排序和分页整合成 SQL 语句及参数。
// SELECT COUNT(*) AS total,cid
// FROM report
// WHERE id > 1000000
// GROUP BY cid
// HAVING
// ORDER BY COUNT(*)
// LIMIT 1
// OFFSET 1
func (s *Search) Parse() (string, []any) {
	if s.raw {
		return s.query, s.args
	}
	var (
		fields       string
		joins        string
		paddingwhere string
		wheres       []string
		groupby      string
		having       string
		orderby      string
		limit        string
		offset       string
	)
	fieldList := append([]string(nil), s.fields...)
	whereConditions := append([]WhereCon(nil), s.whereConditions...)
	if s.table.tableColumns[s.tableName].HaveColumn(IsDeleted) {
		// 这里用局部副本拼接条件，避免 Parse 被多次调用时反复污染原始 Search 状态。

		whereConditions = append(whereConditions, WhereCon{Query: s.table.Name() + ".is_deleted = ?", Args: []any{0}})
	}
	s.query = ""
	s.args = []any{}
	if len(fieldList) == 0 {
		fields = "*"
	} else {
		for i := range fieldList {
			var tableName string
			fieldList[i], tableName, _ = s.warpField(fieldList[i])
			if tableName != s.tableName {
				if !s.joinConditions.HaveTable(tableName) {
					s.Joins(tableName)
				}
			}
		}
		fields = strings.Join(fieldList, ",")
	}
	for _, joincon := range s.joinConditions {
		joins += fmt.Sprintf(" LEFT JOIN %s ON %s", joincon.TableName, joincon.Condition)
	}
	for _, wherecon := range whereConditions {
		paddingwhere = " WHERE "
		wheres = append(wheres, wherecon.Query)
		s.args = append(s.args, wherecon.Args...)
	}
	if len(s.groupConditions) > 0 {
		groupby = " GROUP BY " + strings.Join(s.groupConditions, ",")
	}
	if len(s.havingConditions) > 0 {
		hcs := []string{}
		for _, c := range s.havingConditions {
			hcs = append(hcs, c.Query)
			s.args = append(s.args, c.Args...)
		}
		having = " HAVING " + strings.Join(hcs, " AND ")
	}
	if len(s.orderbyConditions) > 0 {
		orderby = " ORDER BY " + strings.Join(s.orderbyConditions, ",")
	}
	if s.limit != nil {
		limit = " LIMIT ?"
		s.args = append(s.args, s.limit)
	}
	if s.offset != nil {
		offset = " OFFSET ?"
		s.args = append(s.args, s.offset)
	}
	s.query = fmt.Sprintf("SELECT %s FROM `%s`%s%s%s%s%s%s%s%s",
		fields,
		s.tableName,
		joins,
		paddingwhere,
		strings.Join(wheres, " AND "),
		groupby,
		having,
		orderby,
		limit,
		offset,
	)
	// 如果table进行搜索了(table.RowsMap())，那么table下面所有的条件都会一直使用之前的搜索语句。
	// s.raw = true
	return s.query, s.args
}

// warpField 包装查询字段名，自动给已知表字段添加反引号并返回表名和字段名。
// 支持以下字段形式：
// DISTINCT XX
// DISTICT XXX.XXX AS aaa
// XXX.XXX AS aaa
// COUNT(*) AS total
// tablename.*
// DATE_FORMAT(repair.createdtime,'%Y-%m-%d') AS dt
func (s *Search) warpField(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, " ") {
		if strings.Contains(field, "AS") {
			// XXX AS XXX
			sp := strings.Split(field, " ")
			for i := range sp {
				if sp[i] == "AS" {
					sp[i-1], tablename, fieldname = s.warpFieldSingle(sp[i-1])
					warpStr = strings.Join(sp, " ")
					break
				}
			}
		} else {
			sp := strings.Split(field, " ")
			sp[len(sp)-1], tablename, fieldname = s.warpFieldSingle(sp[len(sp)-1])
			warpStr = strings.Join(sp, " ")
		}
	} else {
		return s.warpFieldSingle(field)
	}
	return
}

// warpFieldSingle 包装不含空格的单个字段表达式，自动补齐当前表名并添加反引号。
// 支持 xxx、xxx.xxx、*、COUNT(*)、tablename.* 等形式。
// 单个属性 id
// 表名.属性
// 表名.*
// COUNT(1)之类的函数
// DATE_FORMAT(repair.createdtime,'%Y-%m-%d')
func (s *Search) warpFieldSingle(field string) (warpStr string, tablename string, fieldname string) {
	if strings.Contains(field, ".") {
		sp := strings.Split(field, ".")
		tablename = sp[0]
		fieldname = sp[1]
		if tablename == "" {
			tablename = s.tableName
		}
		if fieldname == "" {
			fieldname = "*"
		}
		tablenameCombine := tablename
		fieldnameCombine := fieldname

		if !strings.Contains(tablename, "`") {
			tablenameCombine = "`" + tablename + "`"
		} else {
			tablename = strings.Replace(tablename, "`", "", -1)
		}

		if !strings.Contains(fieldname, "`") && fieldname != "*" {
			fieldnameCombine = "`" + fieldname + "`"
		} else {
			fieldname = strings.Replace(fieldname, "`", "", -1)
		}

		if s.table.DataBase.HaveTable(tablename) && s.table.DataBase.Table(tablename).HaveColumn(fieldname) {
			warpStr = tablenameCombine + "." + fieldnameCombine
		} else {
			warpStr = field
		}

	} else {
		// 如果没有.
		tablename = s.tableName
		fieldname = field
		warpStr = field
		cols := s.table.DataBase.getColumns(tablename)
		for _, col := range cols {
			if col.Name == field {
				warpStr = "`" + tablename + "`.`" + field + "`"
				break
			}
		}
	}
	return
}

//结果展示

// RowMap 执行查询并返回第一行字符串键值映射；无需查询时返回空 RowMap。
func (s *Search) RowMap() RowMap {
	if s.noNeedQuery {
		return RowMap{}
	}
	nb := (*s).Clone()
	query, args := nb.Parse()
	return s.table.Query(query, args...).RowMap()
}

// Explain 执行 EXPLAIN 查询并返回解析后的执行计划；debug 为 true 时打印 SQL 信息。
func (s *Search) Explain(debug bool) Explain {
	query, args := s.Parse()
	r := s.table.Query("EXPLAIN "+query, args...).RowMap()
	if debug {
		fmt.Println(query)
		fmt.Println(args)
		fmt.Println(getFullSQL(query, args...))
	}
	e := Explain{
		ID:           r.Int("id"),
		SelectType:   r["select_type"],
		Table:        r["table"],
		Partitions:   r["partitions"],
		Type:         r["type"],
		PossibleKeys: r["possible_keys"],
		Key:          r["key"],
		KeyLen:       r.Int("key_len"),
		Ref:          r["ref"],
		Rows:         r.Int("rows"),
		Filtered:     r.Int("filtered"),
		Extra:        r["extra"],
	}
	return e
}

// RowsMap 执行查询并返回多行字符串键值映射；无需查询时返回空 RowsMap。
func (s *Search) RowsMap() RowsMap {
	if s.noNeedQuery {
		return RowsMap{}
	}
	query, args := s.Parse()
	return s.table.Query(query, args...).RowsMap()
}

// RowMapInterface 执行查询并返回第一行 interface 键值映射；无需查询时返回空 RowMapInterface。
func (s *Search) RowMapInterface() RowMapInterface {
	if s.noNeedQuery {
		return RowMapInterface{}
	}
	query, args := s.Parse()
	return s.table.Query(query, args...).RowMapInterface()
}

// RowsMapInterface 执行查询并返回多行 interface 键值映射；无需查询时返回空 RowsMapInterface。
func (s *Search) RowsMapInterface() RowsMapInterface {
	if s.noNeedQuery {
		return RowsMapInterface{}
	}
	query, args := s.Parse()
	return s.table.Query(query, args...).RowsMapInterface()
}

// RowsMapNull 执行查询并返回多行 interface 键值映射，NULL 值用 nil 替代而不是空字符串。
func (s *Search) RowsMapNull() RowsMapInterface {
	if s.noNeedQuery {
		return RowsMapInterface{}
	}
	query, args := s.Parse()
	return s.table.Query(query, args...).RowsMapNull()
}

// DoubleSlice 执行查询并返回字段索引和二维字符串结果；无需查询时返回空结果。
func (s *Search) DoubleSlice() (map[string]int, [][]string) {
	if s.noNeedQuery {
		return map[string]int{}, [][]string{}
	}
	query, args := s.Parse()
	return s.table.Query(query, args...).DoubleSlice()
}

// Int 执行查询并返回单个 int 值；指定字段时读取该字段，否则读取第一行的第一个字段。
func (s *Search) Int(args ...string) int {
	row := s.RowMap()
	if len(args) == 0 {
		for _, v := range row {
			i, _ := strconv.Atoi(v)
			return i
		}
	} else {
		i, _ := strconv.Atoi(row[args[0]])
		return i
	}
	return 0
}

// String 执行查询并返回单个 string 值；指定字段时读取该字段，否则读取第一行的第一个字段。
func (s *Search) String(args ...string) string {
	row := s.RowMap()
	if len(args) == 0 {
		for _, v := range row {
			return v
		}
	} else {
		return row[args[0]]
	}
	return ""
}

// Float 执行查询并返回单个 float64 值；指定字段时读取该字段，否则读取第一行的第一个字段。
func (s *Search) Float(args ...string) float64 {
	row := s.RowMap()
	val := "0"
	if len(args) == 0 {
		for _, v := range row {
			val = v
			break
		}
	} else {
		val = row[args[0]]
	}
	f, _ := strconv.ParseFloat(val, 64)
	return f
}

// Bool 执行查询并返回单个 bool 值；指定字段时读取该字段，否则读取第一行的第一个字段。
func (s *Search) Bool(args ...string) bool {
	row := s.RowMap()
	return row.Bool(args...)
}

// Deprecated: 请使用Struct代替
// Finds 执行查询并将结果填充到传入结构体或结构体切片中。
func (s *Search) Finds(v any) error {
	query, args := s.Parse()
	return s.table.FindAll(v, append([]any{query}, args...)...)
}
