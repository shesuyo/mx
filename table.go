package mx

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Table 表结构体
type Table struct {
	*DataBase
	*Search
	tableName string
	Columns   Columns
	indexs    map[string]Indexs
}

// Name 返回名称
func (t *Table) Name() string {
	return t.tableName
}

// HaveColumn 是否有这个列
func (t *Table) HaveColumn(key string) bool {
	return t.Columns.HaveColumn(key)
}

// UpdateTime 查找表的更新时间
func (t *Table) UpdateTime() string {
	return t.Query("SELECT `UPDATE_TIME` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", t.Schema, t.tableName).String()

}

// AutoIncrement 查找表的自增ID的值
func (t *Table) AutoIncrement() int {
	return t.Query("SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", t.Schema, t.tableName).Int()
}

// SetAutoIncrement 设置自动增长ID
func (t *Table) SetAutoIncrement(id int) error {
	_, err := t.Exec("ALTER TABLE `" + t.tableName + "` AUTO_INCREMENT = " + strconv.Itoa(id)).RowsAffected()
	return err
}

// MaxID 查找表的最大ID，如果为NULL的话则为0
func (t *Table) MaxID() int {
	return t.Query("SELECT IFNULL(MAX(id), 0) as id FROM `" + t.tableName + "`").Int()

}

type Index struct {
	Table        string
	NonUnique    int
	KeyName      string
	SeqInIndex   int
	ColumnName   string
	Collation    string
	Cardinality  int
	Subpart      string // int default null
	Packed       string
	Null         string
	IndexType    string
	Comment      string
	IndexComment string
	Visible      string
	Expression   string
}
type Indexs []Index

func (idxs Indexs) IsUnique() bool {
	for _, idx := range idxs {
		return idx.NonUnique == 0
	}
	return false
}

func (t *Table) Indexs() map[string]Indexs {
	if t.indexs != nil {
		return t.indexs
	}
	indexs := make(map[string]Indexs, 0)
	ms := t.Query(fmt.Sprintf("SHOW INDEX FROM `%s`", t.tableName)).RowsMap().MapIndexs("Key_name")
	for name, rs := range ms {
		rs.SortInt("Seq_in_index", false)
		is := make([]Index, 0)
		for _, r := range rs {
			if r["Non_unique"] == "" {
				fmt.Println("Non_unique缺失")
			}
			is = append(is, Index{
				Table:        r["table"],
				NonUnique:    r.Int("Non_unique"),
				KeyName:      r["Key_name"],
				SeqInIndex:   r.Int("Seq_in_index"),
				ColumnName:   r["Column_name"],
				Collation:    r["Collation"],
				Cardinality:  r.Int("Cardinality"),
				Subpart:      r["Sub_part"],
				Packed:       r["Packed"],
				Null:         r["Null"],
				IndexType:    r["Index_type"],
				Comment:      r["Comment"],
				IndexComment: r["Index_comment"],
				Visible:      r["Visible"],
				Expression:   r["Expression"],
			})
		}
		indexs[name] = is
	}
	t.indexs = indexs
	return indexs
}

// IDIn 查找多个ID对应的列
func (t *Table) IDIn(ids ...any) *SQLRows {
	if len(ids) == 0 {
		return &SQLRows{}
	}
	return t.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE id in (%s)", t.tableName, argslice(len(ids))), ids...)
}

// IDRow 根据ID返回RowMap
func (t *Table) IDRow(id any) RowMap {
	return t.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE id=?", t.tableName), id).RowMap()
}

/*
	map[string]interface{} 增删改查
*/
type SaveResp struct {
	ID         int
	RowsAffect int
}

func (t *Table) Save(obj any) (rsp *SaveResp, err error) {
	rsp = &SaveResp{}
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return rsp, ErrMustBeAddr
	}
	switch v.Elem().Kind() {
	case reflect.Struct:
		// 没有id一定是创建，有id且不会0才是更新。
		isCreate := true
		rID := v.Elem().FieldByName("ID")
		if rID.IsValid() {
			if Int(rID.Interface()) != 0 {
				isCreate = false
			}
		}
		if isCreate {
			if err := callFunc(v, BeforeCreate); err != nil {
				return rsp, err
			}
			m := structToMap(v, t)
			for k, v := range m {
				if k == "id" && v == "" {
					delete(m, "id")
				}
				if (t.Columns[k].DataType == "datetime" || t.Columns[k].DataType == "date") && v == "" {
					delete(m, k)
				}
			}
			id, err := t.Create(m)
			if err != nil {
				return rsp, err
			}
			if rID.IsValid() {
				setReflectValue(rID, stringByte(strconv.Itoa(id)))
			}
			callFunc(v, AfterCreate)
			rsp.ID = id
		} else {
			if err := callFunc(v, BeforeUpdate); err != nil {
				return rsp, err
			}
			m := structToMap(v, t)
			ra, err := t.Update(m)
			if err != nil {
				return rsp, err
			}
			rsp.RowsAffect = ra
			if err := callFunc(v, AfterUpdate); err != nil {
				return rsp, err
			}
		}
	case reflect.Slice:
		if v.Elem().Len() == 0 {
			return rsp, nil
		}
		ve := v.Elem()
		sLen := ve.Len()
		for i := 0; i < sLen; i++ {
			_, err := t.Save(ve.Index(i).Addr().Interface())
			if err != nil {
				return rsp, err
			}
		}
	}

	return rsp, nil
}

// Create 创建
// check 如果有，则会判断表里面以这几个字段为唯一的话，数据库是否存在此条数据，如果有就不插入了。
// 所有ORM的底层。FormXXX， (*DataBase)CRUD
//
func (t *Table) Create(m map[string]any, checks ...string) (int, error) {
	//INSERT INTO `feedback` (`task_id`, `template_question_id`, `question_options_id`, `suggestion`, `member_id`) VALUES ('1', '1', '1', '1', '1')
	if len(checks) > 0 {
		names := []string{}
		values := []any{}
		for _, check := range checks {
			names = append(names, "`"+check+"`"+" = ? ")
			values = append(values, m[check])
		}
		// SELECT COUNT(*) FROM `feedback` WHERE `task_id` = ? AND `member_id` = ?
		if t.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE %s", t.tableName, strings.Join(names, "AND ")), values...).Int() > 0 {
			return 0, ErrInsertRepeat
		}
	}
	ks, vs := ksvs(m)
	id, err := t.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", t.tableName, strings.Join(ks, ","), argslice(len(ks))), vs...).LastInsertId()
	if err != nil {
		return 0, ErrExec
	}
	return int(id), nil
}

// Creates 创建多条数据
func (t *Table) Creates(ms []map[string]any) (int, error) {
	if len(ms) == 0 {
		return 0, nil
	}
	// INSERT INTO `feedback` (`task_id`, `template_question_id`, `question_options_id`, `suggestion`, `member_id`) VALUES ('1', '1', '1', '1', '1'),('1', '1', '1', '1', '1')
	fields := []string{}
	args := []any{}
	sqlFields := []string{}
	sqlArgs := []string{}
	sqlArg := "(" + argslice(len(ms[0])) + ")"
	for i := 0; i < len(ms); i++ {
		sqlArgs = append(sqlArgs, sqlArg)
	}

	for k := range ms[0] {
		fields = append(fields, k)
		sqlFields = append(sqlFields, "`"+k+"`")
	}

	for _, v := range ms {
		for _, field := range fields {
			args = append(args, v[field])
		}
	}
	rows, err := t.Exec(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s ", t.tableName, strings.Join(sqlFields, ","), strings.Join(sqlArgs, ",")), args...).RowsAffected()
	return int(rows), err
}

// DeleteID Delete(map[string]interface{}{"id": id})
func (t *Table) DeleteID(id any) (int, error) {
	return t.Delete(map[string]any{"id": id})
}

// DeleteIDs delete multi id
func (t *Table) DeleteIDs(args ...any) (int, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) == 1 {
		args = expandSlice(args[0])
	}
	if len(args) == 0 {
		return 0, nil
	}
	af, err := t.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE id IN(%s)", t.tableName, placeholder(len(args))), args...).RowsAffected()
	return int(af), err
}

// Delete 删除
func (t *Table) Delete(m map[string]any) (int, error) {
	if len(m) == 0 {
		return 0, ErrNoDeleteKey
	}
	ks, vs := ksvs(m, " = ? ")
	if t.tableColumns[t.tableName].HaveColumn(IsDeleted) {
		af, err := t.Exec(fmt.Sprintf("UPDATE `%s` SET is_deleted = '1', deleted_at = '%s' WHERE %s", t.tableName, time.Now().Format(TimeFormat), strings.Join(ks, "AND")), vs...).RowsAffected()
		return int(af), err
	}
	af, err := t.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RowsAffected()
	return int(af), err
}

// Update 更新
// 如果map里面有id的话会自动删除id，然后使用id来作为更新的条件。
func (t *Table) Update(m map[string]any, keys ...string) (int, error) {
	id := m["id"]
	if len(keys) == 0 {
		keys = append(keys, "id")
	}
	keysValue := []any{}
	whereks := []string{}
	for _, key := range keys {
		val, ok := m[key]
		if !ok {
			return 0, ErrNoUpdateKey
		}
		keysValue = append(keysValue, val)
		delete(m, key)
		whereks = append(whereks, "`"+key+"` = ? ")
	}
	// 因为在更新的时候最好不要更新id，而有时候又会将id传入进来，所以id每次都会被删除，如果要更新id的话使用Exec(),但强烈不推荐修改id！
	delete(m, "id")
	ks, vs := ksvs(m, " = ? ")
	for _, val := range keysValue {
		vs = append(vs, val)
	}
	m["id"] = id
	ra, err := t.Exec(
		fmt.Sprintf("UPDATE `%s` SET %s WHERE %s LIMIT 1",
			t.tableName,
			strings.Join(ks, ","),
			strings.Join(whereks, "AND"),
		),
		vs...,
	).RowsAffected()
	if err != nil {
		// 可能是语法，也可能是执行错误。
		return 0, ErrExec
	}
	return int(ra), err
}

// CreateOrUpdate 创建或者更新
func (t *Table) CreateOrUpdate(m map[string]any, keys ...string) error {
	_, err := t.Create(m, keys...)
	if err != nil {
		if err == ErrInsertRepeat {
			// 在len(map) <= len(keys)的时候可以不用执行更新操作，因为没有任何东西需要更新。
			if len(m) > len(keys) {
				_, err := t.Update(m, keys...)
				return err
			}
			return nil
		}
		return err
	}
	return nil
}

// Read 查找单条数据
func (t *Table) Read(m map[string]any) RowMap {
	rs := t.Reads(m)
	if len(rs) > 0 {
		return rs[0]
	}
	return RowMap{}
}

// Reads 查找多条数据
func (t *Table) Reads(m map[string]any) RowsMap {
	if t.tableColumns[t.tableName].HaveColumn(IsDeleted) {
		m[IsDeleted] = 0
	}
	//SELECT * FROM address WHERE id = 1 AND uid = 27
	ks, vs := ksvs(m, " = ? ")
	return t.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s", t.tableName, strings.Join(ks, "AND")), vs...).RowsMap()
}

// Clone 克隆
// 克隆要保证状态在每个链式操作后都是独立的。
func (t *Table) Clone() *Table {
	newTable := &Table{
		DataBase:  t.DataBase,
		tableName: t.tableName,
	}
	if t.Search == nil {
		newTable.Search = &Search{table: newTable, tableName: t.tableName}
	} else {
		newTable.Search = t.Search.Clone()
		newTable.Search.table = newTable
	}
	return newTable
}

// Where field = arg
func (t *Table) Where(query string, args ...any) *Table {
	return t.Clone().Search.Where(query, args...).table
}

// WhereNotEmpty if arg empty,will do nothing
func (t *Table) WhereNotEmpty(query, arg string) *Table {
	if arg == "" {
		return t
	}
	return t.Clone().Search.Where(query, arg).table
}

// WhereTime where time
func (t *Table) WhereTime(field string, tm Time) *Table {
	tm.Parse()
	if tm.St == "" && tm.Et == "" {
		return t
	}
	nt := t.Clone().Search.Where(fmt.Sprintf("%s >= ? AND %s <= ?", field, field), tm.St, tm.Et).table
	if tm.Stime != "" {
		nt = nt.WhereStartEndTime(field, tm.Stime, tm.Etime)
	}
	return nt
}

// WherePeriod  [st,et)
func (t *Table) WherePeriod(field, st, et string) *Table {
	sp, ep, err := periodParse(st, et)
	if err != nil {
		return t
	}
	return t.Clone().Search.Where(fmt.Sprintf("%s >= ? AND %s < ?", field, field), sp, ep).table
}

// WhereStartEndDay DATE_FORMAT(field, '%Y-%m-%d') >= startTime AND DATE_FORMAT(field, '%Y-%m-%d') <= endTime
// if startDay == "", will do nothing
// if endDay == "", endDay = startDay
// '','' => return
// '2017-07-01', '' => '2017-07-01', '2017-07-01'
// '', '2017-07-02' => '','2017-07-02' (TODO)
// '2017-07-01','2017-07-02' => '2017-07-02','2017-07-01'
func (t *Table) WhereStartEndDay(field, startDay, endDay string) *Table {
	if startDay == "" && endDay == "" {
		return t
	}
	if startDay != "" && endDay == "" {
		endDay = startDay
	}
	startDay += " 00:00:00"
	endDay += " 23:59:59"
	return t.Clone().Search.Where(fmt.Sprintf("%s >= ? AND %s <= ?", field, field), startDay, endDay).table
}

// WhereStartEndMonth DATE_FORMAT(field, '%Y-%m') >= startMonth AND DATE_FORMAT(field, '%Y-%m') <= endMonth
// if startMonth == "", will do nothing
// if endMonth == "", endMonth = startMonth
func (t *Table) WhereStartEndMonth(field, startMonth, endMonth string) *Table {
	if startMonth == "" && endMonth == "" {
		return t
	}
	if startMonth != "" && endMonth == "" {
		endMonth = startMonth
	}
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m') >= ? AND DATE_FORMAT("+field+",'%Y-%m') <= ?", startMonth, endMonth).table
}

// WhereStartEndTime DATE_FORMAT(field, '%H:%i') >= startTime AND DATE_FORMAT(field, '%H:%i') <= endTime
// if startTime == "", will do nothing
// if endTime == "", endTime = startTime
func (t *Table) WhereStartEndTime(field, startTime, endTime string) *Table {
	if startTime == "" && endTime == "" {
		return t
	}
	if startTime != "" && endTime == "" {
		endTime = startTime
	}
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%H:%i') >= ? AND DATE_FORMAT("+field+",'%H:%i') <= ?", startTime, endTime).table
}

// WhereToday DATE_FORMAT(field, '%Y-%m-%d') = {today}
// (field >= '2017-01-01 00:00:00' AND %s < '2017-01-02 00:00:00' )
func (t *Table) WhereToday(field string) *Table {
	return t.WhereDay(field, time.Now().Format("2006-01-02"))
}

// WhereDay DATE_FORMAT(field, '%Y-%m-%d') = day
func (t *Table) WhereDay(field, day string) *Table {
	return t.Clone().Search.Where(WhereTimeParse(field, day, 0, 0, 1)).table
}

// WhereMonth DATE_FORMAT(field, '%Y-%m') = month
func (t *Table) WhereMonth(field, month string) *Table {
	return t.Clone().Search.Where(WhereTimeParse(field, month, 0, 1, 0)).table
}

// WhereBeforeToday DATE_FORMAT(field, '%Y-%m-%d') < {today}
func (t *Table) WhereBeforeToday(field string) *Table {
	return t.Clone().Search.Where("DATE_FORMAT("+field+",'%Y-%m-%d') < ?", time.Now().Format("2006-01-02")).table
}

// WhereLike field LIKE %like%
// If like == "", will do nothing.
func (t *Table) WhereLike(field, like string) *Table {
	if like == "" {
		return t
	}
	return t.Clone().Search.Where(field+" LIKE ?", "%"+like+"%").table
}

// WhereLikeLeft field LIKE %like
func (t *Table) WhereLikeLeft(field, like string) *Table {
	if like == "" {
		return t
	}
	return t.Clone().Search.Where(field+" LIKE ?", "%"+like).table
}

// WhereLikeRight field LIKE like%
func (t *Table) WhereLikeRight(field, like string) *Table {
	if like == "" {
		return t
	}
	return t.Clone().Search.Where(field+" LIKE ?", like+"%").table
}

// WhereID id = ?
func (t *Table) WhereID(id any) *Table {
	return t.Clone().Search.WhereID(id).table
}

// In In(field, a,b,c)
func (t *Table) In(field string, args ...any) *Table {
	return t.Clone().Search.In(field, args...).table
}

// InWhere In(field, 1,2,3) to Where("field >= min AND field <= max")
func (t *Table) InWhere(field string, args ...int) *Table {
	min := math.MaxInt
	max := math.MinInt
	for _, arg := range args {
		if arg < min {
			min = arg
		}
		if arg > max {
			max = arg
		}
	}
	return t.Clone().Search.Where(fmt.Sprintf("%s >=? AND %s <=?", field, field), min, max).table
}

// MustIn 强制搜索结果一定要包含args里面的数据，如果为空则返回空数据。
func (t *Table) MustIn(field string, args ...any) *Table {
	return t.Clone().Search.MustIn(field, args...).table
}

// NotIn not in
func (t *Table) NotIn(field string, args ...any) *Table {
	return t.Clone().Search.NotIn(field, args...).table
}

// Joins LEFT JOIN
// with auto join map
func (t *Table) Joins(query string, args ...string) *Table {
	return t.Clone().Search.Joins(query, args...).table
}

// OrderBy ORDER BY
func (t *Table) OrderBy(field string, isDESC ...bool) *Table {
	return t.Clone().Search.OrderBy(field, isDESC...).table
}

// Limit LIMIT
func (t *Table) Limit(n any) *Table {
	return t.Clone().Search.Limit(n).table
}

// Offset OFFSET
func (t *Table) Offset(n any) *Table {
	return t.Clone().Search.Offset(n).table
}

// Page page => limit & offset
func (t *Table) Page(limit, page int) *Table {
	offset := (page - 1) * limit
	if offset < 0 {
		offset = 0
	}
	return t.Clone().Search.Limit(limit).Offset(offset).table
}

// Fields fields
func (t *Table) Fields(args ...string) *Table {
	if len(args) == 0 {
		return t
	}
	return t.Clone().Search.Fields(args...).table
}

// FieldCount equal Fields("COUNT(*) AS total")
func (t *Table) FieldCount(as ...string) *Table {
	asWhat := "total"
	if len(as) > 0 {
		sp := strings.Split(as[0], " ")
		asWhat = sp[0]
	}
	return t.Clone().Search.Fields("COUNT(*) AS " + asWhat).table
}

// Group GROUP BY
func (t *Table) Group(fields ...string) *Table {
	return t.Clone().Search.Group(fields...).table
}

// Having Having
func (t *Table) Having(query string, args ...any) *Table {
	return t.Clone().Search.Having(query, args...).table
}

// Count count
func (t *Table) Count() int {
	s := t.Clone().Search
	var count int
	s.fields = []string{"COUNT(*)"}
	query, args := s.Parse()
	s.table.Query(query, args...).Find(&count)
	return count
}

// FullMember
// 传入进来的数组，是这个关键属性的所有成员。
// 如果没有关键属性，则关键属性过滤后为整个表。
func (t *Table) FullMember(members []map[string]string, group string, groupValue any, key string) error {
	var err error
	memberCheckMap := make(map[string]bool, len(members))
	for _, member := range members {
		memberCheckMap[member[key]] = true
	}
	oData := t.Fields("id", key).Where(group+" = ?", groupValue).RowsMap()
	oldCheckMap := make(map[string]bool, len(oData))
	for _, r := range oData {
		oldCheckMap[r[key]] = true
	}
	for _, r := range oData {
		if !memberCheckMap[r[key]] {
			_, err = t.Delete(map[string]any{"id": r["id"]})
			if err != nil {
				return err
			}
		}
	}
	for _, r := range members {
		if !oldCheckMap[r[key]] {
			m := make(map[string]any, 0)
			for k, v := range r {
				m[k] = v
			}
			m[group] = groupValue
			_, err = t.Create(m)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func setStruct(v reflect.Value, t reflect.Type, cols map[string]int, data [][]byte) (err error) {
	for i := 0; i < t.NumField(); i++ {
		// mx json toDBName(fieldName)
		var (
			dbFieldName string
			f           = t.Field(i)
			sn          = f.Name
			tagMx       = f.Tag.Get("mx")
		)
		if tagMx != "" {
			if tagMx == "-" {
				continue
			} else {
				dbFieldName = tagMx
			}
		}
		if f.Anonymous {
			embedV := v.FieldByName(sn)
			setStruct(embedV, embedV.Type(), cols, data)
		} else {
			if dbFieldName == "" {
				tagJSON := f.Tag.Get("json")
				if tagJSON != "" {
					if strings.Contains(tagJSON, ",") {
						dbFieldName = strings.Split(tagJSON, ",")[0]
					} else {
						dbFieldName = tagJSON
					}
				} else {
					dbFieldName = toDBName(sn)
				}
			}
			if dataIdx, ok := cols[dbFieldName]; ok {
				// fmt.Println("SET:", sn, dbFieldName, string(data[cols[dbFieldName]]))
				setReflectValue(v.FieldByName(sn), data[dataIdx])
			} else {
				// reflect.Type.Type 是名字 例如 main.Weapon
				// reflect.Value.Kind() 是类型
				// nss: not set struct/slice
				// fmt.Println("NOT SET:", sn, f.Type, v.Kind() == reflect.Struct, dbFieldName)
			}
		}
	}
	return
}

func setSlice(v reflect.Value, t reflect.Type, cols map[string]int, data [][][]byte) (err error) {
	t = v.Type().Elem()
	for k := 0; k < len(data); k++ {
		rnp := reflect.New(t)
		rn := rnp.Elem()
		for i := 0; i < t.NumField(); i++ {
			// mx json toDBName(fieldName)
			var (
				dbFieldName string
				f           = t.Field(i)
				sn          = f.Name
				tagMx       = f.Tag.Get("mx")
			)
			if tagMx != "" {
				if tagMx == "-" {
					continue
				} else {
					dbFieldName = tagMx
				}
			}
			if f.Anonymous {
				embedV := rn.FieldByName(sn)
				setStruct(embedV, embedV.Type(), cols, data[k])
			} else {
				if dbFieldName == "" {
					tagJSON := f.Tag.Get("json")
					if tagJSON != "" {
						if strings.Contains(tagJSON, ",") {
							dbFieldName = strings.Split(tagJSON, ",")[0]
						} else {
							dbFieldName = tagJSON
						}
					} else {
						dbFieldName = toDBName(sn)
					}
				}
				if dataIdx, ok := cols[dbFieldName]; ok {
					// fmt.Println("SET:", sn, dbFieldName, string(data[cols[dbFieldName]]))
					setReflectValue(rn.FieldByName(sn), data[k][dataIdx])
				} else {
					// reflect.Type.Type 是名字 例如 main.Weapon
					// reflect.Value.Kind() 是类型
					// nss: not set struct/slice
					// fmt.Println("NOT SET:", sn, f.Type, v.Kind() == reflect.Struct, dbFieldName)
				}
			}
		}
		v.Set(reflect.Append(v, rn))
	}
	return
}

func (ms *ModelStruct) setSlice(t *Table, cols map[string]int, datas [][][]byte, guess bool) (err error) {
	rt := ms.rv.Type().Elem()
	numField := rt.NumField()
	for j := 0; j < len(datas); j++ {
		rnp := reflect.New(rt)
		rn := rnp.Elem()
		for i := 0; i < numField; i++ {
			// mx json toDBName(fieldName)
			var (
				dbFieldName string
				f           = rt.Field(i)
				sn          = f.Name
				tagMx       = f.Tag.Get("mx")
			)
			if tagMx != "" && tagMx != "-" {
				dbFieldName = tagMx
			}
			if f.Anonymous {
				embedV := rn.FieldByName(sn)
				setStruct(embedV, embedV.Type(), cols, datas[j])
			} else {
				if dbFieldName == "" {
					tagJSON := f.Tag.Get("json")
					if tagJSON != "" {
						if strings.Contains(tagJSON, ",") {
							dbFieldName = strings.Split(tagJSON, ",")[0]
						} else {
							dbFieldName = tagJSON
						}
					} else {
						dbFieldName = toDBName(sn)
					}
				}
				if dataIdx, ok := cols[dbFieldName]; ok {
					setReflectValue(rn.FieldByName(sn), datas[j][dataIdx])
				} else if guess {
					unsetValue := rn.FieldByName(sn)
					if t.haveTablename(dbFieldName) {
						subTable := t.Table(dbFieldName)
						key := ""
						guessKey1 := t.tableName + "_id"
						guessKey2 := t.tableName + "id"
						if subTable.HaveColumn(guessKey1) {
							key = guessKey1
						} else if subTable.HaveColumn(guessKey2) {
							key = guessKey2
						}
						if key != "" {
							switch unsetValue.Kind() {
							case reflect.Struct:
								if err := t.Table(dbFieldName).Where(key+" = ?", Int(datas[j][cols["id"]])).Limit(1).ToStruct(unsetValue.Addr().Interface()); err != nil {
									return err
								}
							case reflect.Slice:
								if err := t.Table(dbFieldName).Where(key+" = ?", Int(datas[j][cols["id"]])).ToStruct(unsetValue.Addr().Interface()); err != nil {
									return err
								}
							}
						}
					}
				}
			}
		}
		if method := rnp.MethodByName(AfterFind); method.IsValid() {
			method.Call(nil)
		}
		ms.rv.Set(reflect.Append(ms.rv, rn))
	}
	return nil
}

// Struct no auto query
func (t *Table) Struct(v any) error {
	s := t.Search.Clone()
	query, args := s.Parse()
	cols, data := t.Query(query, args...).TripleByte()
	ms, err := NewModelStruct(v)
	if err != nil {
		return err
	}
	switch ms.rt.Kind() {
	case reflect.Struct:
		if len(data) > 0 {
			ms.SetStruct(t, cols, data[0], false)
		}
	case reflect.Slice:
		if len(data) > 0 {
			ms.setSlice(t, cols, data, false)
		}
	default:
		return errors.New("Unsupport Type " + ms.rt.Kind().String())
	}
	return nil
}

// ToStruct auto query
func (t *Table) ToStruct(v any) error {
	s := t.Search.Clone()
	query, args := s.Parse()
	cols, data := t.Query(query, args...).TripleByte()
	ms, err := NewModelStruct(v)
	if err != nil {
		return err
	}
	switch ms.rt.Kind() {
	case reflect.Struct:
		if len(data) > 0 {
			ms.SetStruct(t, cols, data[0], true)
		}
	case reflect.Slice:
		if len(data) > 0 {
			ms.setSlice(t, cols, data, true)
		}
	default:
		return errors.New("Unsupport Type " + ms.rt.Kind().String())
	}
	return nil
}

type ModelStruct struct {
	v   any
	rvp reflect.Value
	rv  reflect.Value
	rt  reflect.Type
}

func NewModelStruct(v any) (*ModelStruct, error) {
	ms := &ModelStruct{
		v: v,
	}
	ms.rvp = reflect.ValueOf(v)
	if ms.rvp.Kind() != reflect.Ptr {
		return nil, errors.New("Value Can't Addr.")
	}
	ms.rv = ms.rvp.Elem()
	if !ms.rv.CanAddr() {
		return nil, errors.New("Value Can't Addr.")
	}
	ms.rt = ms.rv.Type()
	return ms, nil
}

func (ms *ModelStruct) SetStruct(t *Table, cols map[string]int, data [][]byte, guess bool) error {
	numField := ms.rt.NumField()
	for i := 0; i < numField; i++ {
		// mx json toDBName(fieldName)
		var (
			dbFieldName string
			f           = ms.rt.Field(i)
			sn          = f.Name
			tagMx       = f.Tag.Get("mx")
		)
		if tagMx != "" && tagMx != "-" {
			dbFieldName = tagMx
		} else {
			tagJSON := f.Tag.Get("json")
			if tagJSON != "" {
				if strings.Contains(tagJSON, ",") {
					dbFieldName = strings.Split(tagJSON, ",")[0]
				} else {
					dbFieldName = tagJSON
				}
			} else {
				dbFieldName = toDBName(sn)
			}
		}

		if f.Anonymous {
			embedV := ms.rv.FieldByName(sn)
			setStruct(embedV, embedV.Type(), cols, data)
		} else {
			if dataIdx, ok := cols[dbFieldName]; ok {
				// fmt.Println("SET:", sn, dbFieldName, string(data[cols[dbFieldName]]))
				setReflectValue(ms.rv.FieldByName(sn), data[dataIdx])
			} else if guess {
				// reflect.Type.Type 是名字 例如 main.Weapon
				// reflect.Value.Kind() 是类型
				// nss: not set struct/slice
				// fmt.Println("NOT SET:", sn, f.Type, ms.rv.Kind() == reflect.Struct, dbFieldName)
				unsetValue := ms.rv.FieldByName(sn)
				if t.haveTablename(dbFieldName) {
					subTable := t.Table(dbFieldName)
					key := ""
					guessKey1 := t.tableName + "_id"
					guessKey2 := t.tableName + "id"
					if subTable.HaveColumn(guessKey1) {
						key = guessKey1
					} else if subTable.HaveColumn(guessKey2) {
						key = guessKey2
					}
					if key != "" {
						switch unsetValue.Kind() {
						case reflect.Struct:
							if err := t.Table(dbFieldName).Where(key+" = ?", Int(data[cols["id"]])).Limit(1).ToStruct(unsetValue.Addr().Interface()); err != nil {
								return err
							}
						case reflect.Slice:
							if err := t.Table(dbFieldName).Where(key+" = ?", Int(data[cols["id"]])).ToStruct(unsetValue.Addr().Interface()); err != nil {
								return err
							}
						}
					}
				}

			}
		}
	}
	if af, ok := ms.v.(AfterFinder); ok {
		af.AfterFind()
	}
	return nil
}

func (t *Table) Debug() *Table {
	return t.DebugSwitch(true)
}

func (t *Table) DebugClose() *Table {
	return t.DebugSwitch(false)
}

func (t *Table) DebugSwitch(isDebug bool) *Table {
	t.Search.debug = isDebug
	return t
}

func (t *Table) Query(sql string, args ...any) *SQLRows {
	if t.Search.debug && !t.DataBase.debug {
		mxlog(getFullSQL(sql, args...))
	}
	return t.DataBase.Query(sql, args...)
}

func (t *Table) Exec(sql string, args ...any) *SQLResult {
	if t.Search.debug && !t.DataBase.debug {
		mxlog(getFullSQL(sql, args...))
	}
	return t.DataBase.Exec(sql, args...)
}
