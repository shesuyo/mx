package mx

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

/*
https://dev.mysql.com/doc/refman/8.0/en/keywords.html
(function(){
    var lis = []
	$(".itemizedlist .literal").each(function(i,v){
        lis.push('"'+v.innerText.toLowerCase()+'"')
    })
    console.log(lis.join(','))
    console.log(lis.length)
})()
*/
var keyWords = []string{"accessible", "account", "action", "active", "add", "admin", "after", "against", "aggregate", "algorithm", "all", "alter", "always", "analyse", "analyze", "and", "any", "array", "as", "asc", "ascii", "asensitive", "at", "attribute", "autoextend_size", "auto_increment", "avg", "avg_row_length", "backup", "before", "begin", "between", "bigint", "binary", "binlog", "bit", "blob", "block", "bool", "boolean", "both", "btree", "buckets", "by", "byte", "cache", "call", "cascade", "cascaded", "case", "catalog_name", "chain", "change", "changed", "channel", "char", "character", "charset", "check", "checksum", "cipher", "class_origin", "client", "clone", "close", "coalesce", "code", "collate", "collation", "column", "columns", "column_format", "column_name", "comment", "commit", "committed", "compact", "completion", "component", "compressed", "compression", "concurrent", "condition", "connection", "consistent", "constraint", "constraint_catalog", "constraint_name", "constraint_schema", "contains", "context", "continue", "convert", "cpu", "create", "cross", "cube", "cume_dist", "current", "current_date", "current_time", "current_timestamp", "current_user", "cursor", "cursor_name", "data", "database", "databases", "datafile", "date", "datetime", "day", "day_hour", "day_microsecond", "day_minute", "day_second", "deallocate", "dec", "decimal", "declare", "default", "default_auth", "definer", "definition", "delayed", "delay_key_write", "delete", "dense_rank", "desc", "describe", "description", "des_key_file", "deterministic", "diagnostics", "directory", "disable", "discard", "disk", "distinct", "distinctrow", "div", "do", "double", "drop", "dual", "dumpfile", "duplicate", "dynamic", "each", "else", "elseif", "empty", "enable", "enclosed", "encryption", "end", "ends", "enforced", "engine", "engines", "engine_attribute", "enum", "error", "errors", "escape", "escaped", "event", "events", "every", "except", "exchange", "exclude", "execute", "exists", "exit", "expansion", "expire", "explain", "export", "extended", "extent_size", "failed_login_attempts", "false", "fast", "faults", "fetch", "fields", "file", "file_block_size", "filter", "first", "first_value", "fixed", "float", "float4", "float8", "flush", "following", "follows", "for", "force", "foreign", "format", "found", "from", "full", "fulltext", "function", "general", "generated", "geomcollection", "geometry", "geometrycollection", "get", "get_format", "get_master_public_key", "global", "grant", "grants", "group", "grouping", "groups", "group_replication", "handler", "hash", "having", "help", "high_priority", "histogram", "history", "host", "hosts", "hour", "hour_microsecond", "hour_minute", "hour_second", "identified", "if", "ignore", "ignore_server_ids", "import", "in", "inactive", "index", "indexes", "infile", "initial_size", "inner", "inout", "insensitive", "insert", "insert_method", "install", "instance", "int", "int1", "int2", "int3", "int4", "int8", "integer", "interval", "into", "invisible", "invoker", "io", "io_after_gtids", "io_before_gtids", "io_thread", "ipc", "is", "isolation", "issuer", "iterate", "join", "json", "json_table", "json_value", "key", "keys", "key_block_size", "kill", "lag", "language", "last", "last_value", "lateral", "lead", "leading", "leave", "leaves", "left", "less", "level", "like", "limit", "linear", "lines", "linestring", "list", "load", "local", "localtime", "localtimestamp", "lock", "locked", "locks", "logfile", "logs", "long", "longblob", "longtext", "loop", "low_priority", "master", "master_auto_position", "master_bind", "master_compression_algorithms", "master_connect_retry", "master_delay", "master_heartbeat_period", "master_host", "master_log_file", "master_log_pos", "master_password", "master_port", "master_public_key_path", "master_retry_count", "master_server_id", "master_ssl", "master_ssl_ca", "master_ssl_capath", "master_ssl_cert", "master_ssl_cipher", "master_ssl_crl", "master_ssl_crlpath", "master_ssl_key", "master_ssl_verify_server_cert", "master_tls_ciphersuites", "master_tls_version", "master_user", "master_zstd_compression_level", "match", "maxvalue", "max_connections_per_hour", "max_queries_per_hour", "max_rows", "max_size", "max_updates_per_hour", "max_user_connections", "medium", "mediumblob", "mediumint", "mediumtext", "member", "memory", "merge", "message_text", "microsecond", "middleint", "migrate", "minute", "minute_microsecond", "minute_second", "min_rows", "mod", "mode", "modifies", "modify", "month", "multilinestring", "multipoint", "multipolygon", "mutex", "mysql_errno", "name", "names", "national", "natural", "nchar", "ndb", "ndbcluster", "nested", "network_namespace", "never", "new", "next", "no", "nodegroup", "none", "not", "nowait", "no_wait", "no_write_to_binlog", "nth_value", "ntile", "null", "nulls", "number", "numeric", "nvarchar", "of", "off", "offset", "oj", "old", "on", "one", "only", "open", "optimize", "optimizer_costs", "option", "optional", "optionally", "options", "or", "order", "ordinality", "organization", "others", "out", "outer", "outfile", "over", "owner", "pack_keys", "page", "parser", "partial", "partition", "partitioning", "partitions", "password", "password_lock_time", "path", "percent_rank", "persist", "persist_only", "phase", "plugin", "plugins", "plugin_dir", "point", "polygon", "port", "precedes", "preceding", "precision", "prepare", "preserve", "prev", "primary", "privileges", "privilege_checks_user", "procedure", "process", "processlist", "profile", "profiles", "proxy", "purge", "quarter", "query", "quick", "random", "range", "rank", "read", "reads", "read_only", "read_write", "real", "rebuild", "recover", "recursive", "redofile", "redo_buffer_size", "redundant", "reference", "references", "regexp", "relay", "relaylog", "relay_log_file", "relay_log_pos", "relay_thread", "release", "reload", "remote", "remove", "rename", "reorganize", "repair", "repeat", "repeatable", "replace", "replica", "replicas", "replicate_do_db", "replicate_do_table", "replicate_ignore_db", "replicate_ignore_table", "replicate_rewrite_db", "replicate_wild_do_table", "replicate_wild_ignore_table", "replication", "require", "require_row_format", "reset", "resignal", "resource", "respect", "restart", "restore", "restrict", "resume", "retain", "return", "returned_sqlstate", "returning", "returns", "reuse", "reverse", "revoke", "right", "rlike", "role", "rollback", "rollup", "rotate", "routine", "row", "rows", "row_count", "row_format", "row_number", "rtree", "savepoint", "schedule", "schema", "schemas", "schema_name", "second", "secondary", "secondary_engine", "secondary_engine_attribute", "secondary_load", "secondary_unload", "second_microsecond", "security", "select", "sensitive", "separator", "serial", "serializable", "server", "session", "set", "share", "show", "shutdown", "signal", "signed", "simple", "skip", "slave", "slow", "smallint", "snapshot", "socket", "some", "soname", "sounds", "source", "spatial", "specific", "sql", "sqlexception", "sqlstate", "sqlwarning", "sql_after_gtids", "sql_after_mts_gaps", "sql_before_gtids", "sql_big_result", "sql_buffer_result", "sql_cache", "sql_calc_found_rows", "sql_no_cache", "sql_small_result", "sql_thread", "sql_tsi_day", "sql_tsi_hour", "sql_tsi_minute", "sql_tsi_month", "sql_tsi_quarter", "sql_tsi_second", "sql_tsi_week", "sql_tsi_year", "srid", "ssl", "stacked", "start", "starting", "starts", "stats_auto_recalc", "stats_persistent", "stats_sample_pages", "status", "stop", "storage", "stored", "straight_join", "stream", "string", "subclass_origin", "subject", "subpartition", "subpartitions", "super", "suspend", "swaps", "switches", "system", "table", "tables", "tablespace", "table_checksum", "table_name", "temporary", "temptable", "terminated", "text", "than", "then", "thread_priority", "ties", "time", "timestamp", "timestampadd", "timestampdiff", "tinyblob", "tinyint", "tinytext", "tls", "to", "trailing", "transaction", "trigger", "triggers", "true", "truncate", "type", "types", "unbounded", "uncommitted", "undefined", "undo", "undofile", "undo_buffer_size", "unicode", "uninstall", "union", "unique", "unknown", "unlock", "unsigned", "until", "update", "upgrade", "usage", "use", "user", "user_resources", "use_frm", "using", "utc_date", "utc_time", "utc_timestamp", "validation", "value", "values", "varbinary", "varchar", "varcharacter", "variables", "varying", "vcpu", "view", "virtual", "visible", "wait", "warnings", "week", "weight_string", "when", "where", "while", "window", "with", "without", "work", "wrapper", "write", "x509", "xa", "xid", "xml", "xor", "year", "year_month", "zerofill", "zone", "active", "admin", "array", "attribute", "buckets", "clone", "component", "cume_dist", "definition", "dense_rank", "description", "empty", "enforced", "engine_attribute", "except", "exclude", "failed_login_attempts", "first_value", "following", "geomcollection", "get_master_public_key", "grouping", "groups", "histogram", "history", "inactive", "invisible", "json_table", "json_value", "lag", "last_value", "lateral", "lead", "locked", "master_compression_algorithms", "master_public_key_path", "master_tls_ciphersuites", "master_zstd_compression_level", "member", "nested", "network_namespace", "nowait", "nth_value", "ntile", "nulls", "of", "off", "oj", "old", "optional", "ordinality", "organization", "others", "over", "password_lock_time", "path", "percent_rank", "persist", "persist_only", "preceding", "privilege_checks_user", "process", "random", "rank", "recursive", "reference", "replica", "replicas", "require_row_format", "resource", "respect", "restart", "retain", "returning", "reuse", "role", "row_number", "secondary", "secondary_engine", "secondary_engine_attribute", "secondary_load", "secondary_unload", "skip", "srid", "stream", "system", "thread_priority", "ties", "tls", "unbounded", "vcpu", "visible", "window", "zone", "analyse", "des_key_file", "parse_gcol_expr", "redofile", "sql_cache"}

var (
	fullTitles         = []string{"API", "CPU", "CSS", "CID", "DNS", "EOF", "EPC", "GUID", "HTML", "HTTP", "HTTPS", "ID", "UID", "IP", "JSON", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SSH", "TLS", "TTL", "UI", "UID", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS", "PY"}
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

func ksvs(m map[string]interface{}, keyTail ...string) ([]string, []interface{}) {
	kt := ""
	ks := []string{}
	vs := []interface{}{}
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
		fieleName := t.Field(i).Tag.Get("mx")
		if fieleName != "-" {
			if fieleName == "" {
				fieleName = toDBName(t.Field(i).Name)
			}
			if fieleName != "" && table.HaveColumn(fieleName) {
				tags[i] = fieleName
			}
		}
	}
	structTagMap[structName] = tags
	structTagMu.Unlock()
	return tags
}

// structToMap 将结构体转换成map[string]interface{}
// fields 指定只转换的字段，int不受限制
func structToMap(v reflect.Value, table *Table) map[string]interface{} {
	v = reflect.Indirect(v)
	t := v.Type()
	numField := v.NumField()
	m := make(map[string]interface{}, numField)
	tags := getTags(v, t, table)

	for i := 0; i < numField; i++ {
		if tags[i] == "" {
			continue
		}
		refField := v.Field(i)
		fieldVal := refField.Interface()
		if fieldVal == "" {
			if tags[i] == "id" ||
				table.Columns[tags[i]].DataType == "datetime" {
				continue
			}
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

func stringify(v interface{}) string {
	bs, _ := json.Marshal(&v)
	return string(bs)
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	newm := make(map[string]interface{}, len(m))
	for k, v := range m {
		newm[k] = v
	}
	return newm
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
	etPadding = "2999-12-31 23:59:59"
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
func String(v interface{}) string {
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

// Int 将传入的值转换成int
func Int(v interface{}) int {
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

func expandSlice(arg interface{}) []interface{} {
	args := make([]interface{}, 0)
	if ints, ok := arg.([]int); ok {
		args = make([]interface{}, len(ints))
		for idx, val := range ints {
			args[idx] = val
		}
	} else if strs, ok := arg.([]string); ok {
		args = make([]interface{}, len(strs))
		for idx, val := range strs {
			args[idx] = val
		}
	} else if ins, ok := arg.([]interface{}); ok {
		args = make([]interface{}, len(ins))
		for idx, val := range ins {
			args[idx] = val
		}
	} else if arg == nil {

	} else {
		args = append(args, arg)
	}
	return args
}

func mxlog(args ...interface{}) {
	log.Println(args...)
}
