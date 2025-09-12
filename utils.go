package mx

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	fullTitles         = []string{"API", "CPU", "CSS", "CID", "DNS", "EOF", "EPC", "GUID", "HTML", "HTTP", "HTTPS", "ID", "UID", "IP", "JSON", "QPS", "RAM", "RHS", "RPC", "SLA", "SN", "SMTP", "SSH", "TLS", "TTL", "UI", "UID", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS", "PY"}
	fullTitlesReplacer *strings.Replacer
	structNameMap      map[string]string
	//m和rm公用同一个
	dbNameMap = NewMapStringString()

	placeholders = []string{"", "?", "?,?", "?,?,?", "?,?,?,?", "?,?,?,?,?", "?,?,?,?,?,?", "?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?,?", "?,?,?,?,?,?,?,?,?,?"}
)

// SafeMapStringString 安全的map[string]string
type SafeMapStringString struct {
	m  map[string]string
	mu sync.RWMutex
}

// Get Get
func (safe *SafeMapStringString) Get(key string) (string, bool) {
	safe.mu.RLock()
	val, ok := safe.m[key]
	safe.mu.RUnlock()
	return val, ok
}

// Set Set
func (safe *SafeMapStringString) Set(key, val string) {
	safe.mu.Lock()
	safe.m[key] = val
	safe.mu.Unlock()
}

// NewMapStringString 返回一个安全的map[string]string
func NewMapStringString() *SafeMapStringString {
	safe := new(SafeMapStringString)
	safe.m = make(map[string]string)
	return safe
}

func init() {
	{
		var oldnew []string
		for _, title := range fullTitles {
			oldnew = append(oldnew, title, "_"+strings.ToLower(title))
		}
		for i := 'A'; i <= 'Z'; i++ {
			oldnew = append(oldnew, string(i), "_"+string(i+32))
		}
		fullTitlesReplacer = strings.NewReplacer(oldnew...)
	}
	{
		structNameMap = make(map[string]string, len(fullTitles))
		for _, title := range fullTitles {
			structNameMap[strings.ToLower(title)] = title
		}
	}
}

// ToDBName 将结构体的字段名字转换成对应数据库字段名
func ToDBName(name string) string {
	val, ok := dbNameMap.Get(name)
	if ok {
		return val
	}
	return toDBName(name)
}

// ToStructName 数据库字段名转换成对应结构体名
func ToStructName(name string) string {
	if name == "" {
		return ""
	}

	val, ok := dbNameMap.Get(name)
	if ok {
		return val
	}

	return toStructName(name)
}

func toStructName(name string) string {
	sp := strings.Split(name, "_")
	for i := 0; i < len(sp); i++ {
		val := structNameMap[sp[i]]
		if val == "" {
			if len(sp[i]) > 0 && sp[i][0] >= 'a' && sp[i][0] <= 'z' {
				val = string(sp[i][0]-32) + sp[i][1:]
			}
		}
		sp[i] = val
	}
	structName := strings.Join(sp, "")
	dbNameMap.Set(name, structName)
	return structName
}

func toDBName(name string) string {
	dbName := fullTitlesReplacer.Replace(name)
	if len(dbName) >= 1 {
		dbNameMap.Set(name, dbName[1:])
		dbNameMap.Set(dbName[1:], name)
		return dbName[1:]
	}
	return ""
}

func ksvs(m map[string]any, keyTail ...string) ([]string, []any) {
	kt := ""
	ks := []string{}
	vs := []any{}
	if len(keyTail) > 0 {
		kt = keyTail[0]
	}
	for k, v := range m {
		if expr, ok := v.(Expr); ok {
			state := expr.State
			switch expr.spec {
			case ExprAdd:
				state = fmt.Sprintf("`%s` + ? ", k)
			}
			ks = append(ks, fmt.Sprintf("`%s` = %s", k, state))
			vs = append(vs, expr.Args...)
		} else {
			ks = append(ks, " `"+k+"`"+kt)
			vs = append(vs, v)
		}

	}
	return ks, vs
}

// 用于返回对应个数参数,多用于In。
func argslice(l int) string {
	s := []string{}
	for i := 0; i < l; i++ {
		s = append(s, "?")
	}
	return strings.Join(s, ",")
}

var (
	structTagMu  sync.RWMutex
	structTagMap map[string][]string
)

func init() {
	structTagMap = make(map[string][]string, 19977)
}

func getTags(v reflect.Value, t reflect.Type, table *Table) []string {
	structName := t.String()
	structTagMu.RLock()
	tags, ok := structTagMap[structName]
	structTagMu.RUnlock()
	if ok {
		return tags
	}
	structTagMu.Lock()
	numField := v.NumField()
	tags = make([]string, numField)
	for i := 0; i < numField; i++ {
		fieldName := t.Field(i).Tag.Get("mx")
		if fieldName != "-" {
			if fieldName == "" {
				fieldName = toDBName(t.Field(i).Name)
			}
			if fieldName != "" && table.HaveColumn(fieldName) {
				tags[i] = fieldName
			}
		}
	}
	structTagMap[structName] = tags
	structTagMu.Unlock()
	return tags
}

// structToMap 将结构体转换成map[string]interface{}
// fields 指定只转换的字段，int不受限制
func structToMap(v reflect.Value, table *Table) map[string]any {
	v = reflect.Indirect(v)
	t := v.Type()
	numField := v.NumField()
	m := make(map[string]any, numField)
	tags := getTags(v, t, table)

	for i := 0; i < numField; i++ {
		if tags[i] == "" {
			continue
		}
		refField := v.Field(i)
		fieldVal := refField.Interface()
		if tags[i] == "id" && (fieldVal == "" || fieldVal == 0) {
			continue
		}
		if fieldVal == "" && (table.Columns[tags[i]].DataType == "datetime" || table.Columns[tags[i]].DataType == "date") {
			continue
		}
		if refField.Kind() == reflect.Struct || refField.Kind() == reflect.Slice {
			bs, err := json.Marshal(fieldVal)
			if err == nil {
				fieldVal = byteString(bs)
			} else {
				fieldVal = "null"
			}
		}
		m[tags[i]] = fieldVal
	}

	return m
}

// Placeholder sql占位
// n == 1 return ?
// n == 2 return ?,?
func Placeholder(n int) string {
	return placeholder(n)
}

func placeholder(n int) string {
	if n <= 10 {
		return placeholders[n]
	}
	holder := []string{}
	for i := 0; i < n; i++ {
		holder = append(holder, "?")
	}
	return strings.Join(holder, ",")
}

// MapsToCRUDRows convert []map[string]string to mx.RowsMap
func MapsToCRUDRows(m []map[string]string) RowsMap {
	rm := RowsMap{}
	for _, v := range m {
		rm = append(rm, RowMap(v))
	}
	return rm
}

// WhereTimeParse 将时间段转换成对应SQL [)
func WhereTimeParse(field, ts string, years, months, days int) string {
	// (createdtime >= '2018-01-01 00:00:00' AND createdtime < '2018-01-02 00:00:00')
	var a, b string
	t, _ := timeParse(ts)
	a = t.Format("2006-01-02 15:04:05")
	b = t.AddDate(years, months, days).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("(%s >= '%s' AND %s < '%s')", field, a, field, b)
}

const (
	timeFormat = "2006-01-02 15:04:05"

	stPadding = "2000-01-01 00:00:00"
)

var (
	errPeriodParse = errors.New("period parse err")
)

// timeParse time parse from string
func timeParse(ts string) (time.Time, error) {
	format := timeFormat[:len(ts)]
	t, err := time.ParseInLocation(format, ts, time.Local)
	return t, err
}

func periodParse(st, et string) (string, string, error) {
	stl := len(st)
	etl := len(et)
	if stl == 0 {
		return "", "", errPeriodParse
	}
	sp := st + stPadding[stl:]
	var ep string
	spt, _ := timeParse(sp)
	if etl == 0 {
		bt := spt
		switch stl {
		case 4:
			ep = bt.AddDate(1, 0, 0).Format(timeFormat)
		case 7:
			ep = bt.AddDate(0, 1, 0).Format(timeFormat)
		case 10:
			ep = bt.AddDate(0, 0, 1).Format(timeFormat)
		case 13:
			ep = bt.Add(1 * time.Hour).Format(timeFormat)
		case 16:
			ep = bt.Add(1 * time.Minute).Format(timeFormat)
		case 19:
			ep = bt.Add(1 * time.Second).Format(timeFormat)
		default:
			return "", "", errPeriodParse
		}
	} else {
		if stl != etl {
			return "", "", errPeriodParse
		}
		ept, _ := timeParse(et)
		bt := ept
		switch stl {
		case 4:
			ep = bt.AddDate(1, 0, 0).Format(timeFormat)
		case 7:
			ep = bt.AddDate(0, 1, 0).Format(timeFormat)
		case 10:
			ep = bt.AddDate(0, 0, 1).Format(timeFormat)
		case 13:
			ep = bt.Add(1 * time.Hour).Format(timeFormat)
		case 16:
			ep = bt.Add(1 * time.Minute).Format(timeFormat)
		case 19:
			ep = bt.Add(1 * time.Second).Format(timeFormat)
		default:
			return "", "", errPeriodParse
		}
	}
	return sp, ep, nil
}

func byteString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func stringByte(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// String 将传入的值转换成字符串
func String(v any) string {
	var s string
	switch v := v.(type) {
	case int:
		s = strconv.Itoa(v)
	case int64:
		s = strconv.Itoa(int(v))
	case []byte:
		s = *(*string)(unsafe.Pointer(&v))
	case byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}

func Float(v any) float64 {
	f, _ := strconv.ParseFloat(String(v), 64)
	return f
}

// Int 将传入的值转换成int
func Int(v any) int {
	var i int

	switch v := v.(type) {
	case string:
		i, _ = strconv.Atoi(v)
	// 一个case多个值，就无法确认是什么类型了，就成了interface{}，所以要分开写。
	case int:
		i = v
	case int8:
		i = int(v)
	case int16:
		i = int(v)
	case int32:
		i = int(v)
	case int64:
		i = int(v)
	case uint:
		i = int(v)
	case uint8:
		i = int(v)
	case uint16:
		i = int(v)
	case uint32:
		i = int(v)
	case uint64:
		i = int(v)
	case []byte:
		if len(v) == 0 {
			return 0
		}
		isNagative := false
		startIdx := 0
		if v[0] == '-' {
			startIdx++
			isNagative = true
		}
		for j := startIdx; j < len(v); j++ {
			i *= 10
			i += int(v[j] - '0')
		}
		if isNagative {
			i = -i
		}
	default:
		i, _ = strconv.Atoi(fmt.Sprintf("%v", v))
	}
	return i
}

func callFunc(v reflect.Value, name string) error {
	f := v.MethodByName(name)
	if f.IsValid() {
		vs := f.Call(nil)
		if len(vs) == 1 {
			if err, ok := vs[0].Interface().(error); ok {
				return err
			}
		}
	}
	return nil
}

// Time
// [st,et]
// st et 开始 结束时刻
// sday eday 开始日期 结束日期
// stime etime 每天的这个时刻
type Time struct {
	St     string `json:"st"`
	Et     string `json:"et"`
	Day    string `json:"day"`
	Sday   string `json:"sday"`
	Eday   string `json:"eday"`
	Stime  string `json:"stime"`
	Etime  string `json:"etime"`
	Month  string `json:"month"`
	Smonth string `json:"smonth"`
	Emonth string `json:"emonth"`
}

func (t *Time) Parse() {
	// st et
	// sday eday
	// month
	// day
	if t.Sday != "" && t.Eday == "" {
		t.Eday = t.Sday
	}
	if t.Stime != "" && t.Etime == "" {
		t.Etime = t.Stime
	}
	if t.St == "" {
		if t.Sday != "" {
			t.St = t.Sday + " 00:00:00"
			t.Et = t.Eday + " 23:59:59"
		}
		if t.Day != "" {
			t.St = t.Day + " 00:00:00"
			t.Et = t.Day + " 23:59:59"
		}
		if t.Month != "" {
			month, _ := time.Parse("2006-01", t.Month)
			t.St = month.Format(timeFormat)
			t.Et = month.AddDate(0, 1, 0).Add(-1 * time.Second).Format(timeFormat)
		}
	}
	if t.Smonth != "" {
		if t.Emonth == "" {
			t.Emonth = t.Smonth
		}
		smonth, _ := time.Parse("2006-01", t.Smonth)
		emonth, _ := time.Parse("2006-01", t.Emonth)
		t.St = smonth.Format(timeFormat)
		t.Et = emonth.AddDate(0, 1, 0).Add(-1 * time.Second).Format(timeFormat)
	}
}

func setReflectValue(v reflect.Value, bs []byte) {
	if v.Interface() != nil {
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.Set(reflect.ValueOf(Int(bs)))
		case reflect.String:
			v.Set(reflect.ValueOf(String(bs)))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v.SetUint(uint64(Int(bs)))
		case reflect.Struct, reflect.Slice:
			json.Unmarshal(bs, v.Addr().Interface())
		case reflect.Map:
			json.Unmarshal(bs, v.Addr().Interface())
		case reflect.Float64:
			f, _ := strconv.ParseFloat(String(bs), 64)
			v.SetFloat(f)
		case reflect.Float32:
			f, _ := strconv.ParseFloat(String(bs), 32)
			v.SetFloat(f)
		default:
			v.Set(reflect.ValueOf(String(bs)))
		}
	}
}

func expandSlice(arg any) []any {
	args := make([]any, 0)
	if ints, ok := arg.([]int); ok {
		args = make([]any, len(ints))
		for idx, val := range ints {
			args[idx] = val
		}
	} else if strs, ok := arg.([]string); ok {
		args = make([]any, len(strs))
		for idx, val := range strs {
			args[idx] = val
		}
	} else if ins, ok := arg.([]any); ok {
		args = make([]any, len(ins))
		copy(args, ins)
	} else if arg == nil {

	} else {
		args = append(args, arg)
	}
	return args
}

func mxlog(args ...any) {
	fmt.Println(args...)
}

func getCallers() LogSqlCallers {
	callers := LogSqlCallers{}
	pcs := make([]uintptr, 32)
	// skip getCallers()
	callDep := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:callDep])
	for {
		// frame: {"PC":13310021,"Func":{},"Function":"github.com/shesuyo/mx.getCallers","File":"C:/code/gopath/src/github.com/shesuyo/mx/utils.go","Line":562,"Entry":13309952}
		frame, ok := frames.Next()
		if strings.Index(frame.Function, "github.com/shesuyo/mx") == 0 {
			continue
		}
		callers = append(callers, LogSqlCaller{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
		})
		if frame.Function == "main.main" {
			break
		}
		if !ok {
			break
		}
	}
	return callers
}
