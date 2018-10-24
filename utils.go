package mx

import (
	"encoding/json"
	"fmt"
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
var keyWords = []string{"accessible", "account", "action", "active", "add", "admin", "after", "against", "aggregate", "algorithm", "all", "alter", "always", "analyse", "analyze", "and", "any", "as", "asc", "ascii", "asensitive", "at", "autoextend_size", "auto_increment", "avg", "avg_row_length", "backup", "before", "begin", "between", "bigint", "binary", "binlog", "bit", "blob", "block", "bool", "boolean", "both", "btree", "buckets", "by", "byte", "cache", "call", "cascade", "cascaded", "case", "catalog_name", "chain", "change", "changed", "channel", "char", "character", "charset", "check", "checksum", "cipher", "class_origin", "client", "clone", "close", "coalesce", "code", "collate", "collation", "column", "columns", "column_format", "column_name", "comment", "commit", "committed", "compact", "completion", "component", "compressed", "compression", "concurrent", "condition", "connection", "consistent", "constraint", "constraint_catalog", "constraint_name", "constraint_schema", "contains", "context", "continue", "convert", "cpu", "create", "cross", "cube", "cume_dist", "current", "current_date", "current_time", "current_timestamp", "current_user", "cursor", "cursor_name", "data", "database", "databases", "datafile", "date", "datetime", "day", "day_hour", "day_microsecond", "day_minute", "day_second", "deallocate", "dec", "decimal", "declare", "default", "default_auth", "definer", "definition", "delayed", "delay_key_write", "delete", "dense_rank", "desc", "describe", "description", "des_key_file", "deterministic", "diagnostics", "directory", "disable", "discard", "disk", "distinct", "distinctrow", "div", "do", "double", "drop", "dual", "dumpfile", "duplicate", "dynamic", "each", "else", "elseif", "empty", "enable", "enclosed", "encryption", "end", "ends", "engine", "engines", "enum", "error", "errors", "escape", "escaped", "event", "events", "every", "except", "exchange", "exclude", "execute", "exists", "exit", "expansion", "expire", "explain", "export", "extended", "extent_size", "false", "fast", "faults", "fetch", "fields", "file", "file_block_size", "filter", "first", "first_value", "fixed", "float", "float4", "float8", "flush", "following", "follows", "for", "force", "foreign", "format", "found", "from", "full", "fulltext", "function", "general", "generated", "geomcollection", "geometry", "geometrycollection", "get", "get_format", "get_master_public_key", "global", "grant", "grants", "group", "grouping", "groups", "group_replication", "handler", "hash", "having", "help", "high_priority", "histogram", "history", "host", "hosts", "hour", "hour_microsecond", "hour_minute", "hour_second", "identified", "if", "ignore", "ignore_server_ids", "import", "in", "inactive", "index", "indexes", "infile", "initial_size", "inner", "inout", "insensitive", "insert", "insert_method", "install", "instance", "int", "int1", "int2", "int3", "int4", "int8", "integer", "interval", "into", "invisible", "invoker", "io", "io_after_gtids", "io_before_gtids", "io_thread", "ipc", "is", "isolation", "issuer", "iterate", "join", "json", "json_table", "key", "keys", "key_block_size", "kill", "lag", "language", "last", "last_value", "lateral", "lead", "leading", "leave", "leaves", "left", "less", "level", "like", "limit", "linear", "lines", "linestring", "list", "load", "local", "localtime", "localtimestamp", "lock", "locked", "locks", "logfile", "logs", "long", "longblob", "longtext", "loop", "low_priority", "master", "master_auto_position", "master_bind", "master_connect_retry", "master_delay", "master_heartbeat_period", "master_host", "master_log_file", "master_log_pos", "master_password", "master_port", "master_public_key_path", "master_retry_count", "master_server_id", "master_ssl", "master_ssl_ca", "master_ssl_capath", "master_ssl_cert", "master_ssl_cipher", "master_ssl_crl", "master_ssl_crlpath", "master_ssl_key", "master_ssl_verify_server_cert", "master_tls_version", "master_user", "match", "maxvalue", "max_connections_per_hour", "max_queries_per_hour", "max_rows", "max_size", "max_updates_per_hour", "max_user_connections", "medium", "mediumblob", "mediumint", "mediumtext", "memory", "merge", "message_text", "microsecond", "middleint", "migrate", "minute", "minute_microsecond", "minute_second", "min_rows", "mod", "mode", "modifies", "modify", "month", "multilinestring", "multipoint", "multipolygon", "mutex", "mysql_errno", "name", "names", "national", "natural", "nchar", "ndb", "ndbcluster", "nested", "never", "new", "next", "no", "nodegroup", "none", "not", "nowait", "no_wait", "no_write_to_binlog", "nth_value", "ntile", "null", "nulls", "number", "numeric", "nvarchar", "of", "offset", "old", "on", "one", "only", "open", "optimize", "optimizer_costs", "option", "optional", "optionally", "options", "or", "order", "ordinality", "organization", "others", "out", "outer", "outfile", "over", "owner", "pack_keys", "page", "parser", "partial", "partition", "partitioning", "partitions", "password", "path", "percent_rank", "persist", "persist_only", "phase", "plugin", "plugins", "plugin_dir", "point", "polygon", "port", "precedes", "preceding", "precision", "prepare", "preserve", "prev", "primary", "privileges", "procedure", "process", "processlist", "profile", "profiles", "proxy", "purge", "quarter", "query", "quick", "range", "rank", "read", "reads", "read_only", "read_write", "real", "rebuild", "recover", "recursive", "redofile", "redo_buffer_size", "redundant", "reference", "references", "regexp", "relay", "relaylog", "relay_log_file", "relay_log_pos", "relay_thread", "release", "reload", "remote", "remove", "rename", "reorganize", "repair", "repeat", "repeatable", "replace", "replicate_do_db", "replicate_do_table", "replicate_ignore_db", "replicate_ignore_table", "replicate_rewrite_db", "replicate_wild_do_table", "replicate_wild_ignore_table", "replication", "require", "reset", "resignal", "resource", "respect", "restart", "restore", "restrict", "resume", "retain", "return", "returned_sqlstate", "returns", "reuse", "reverse", "revoke", "right", "rlike", "role", "rollback", "rollup", "rotate", "routine", "row", "rows", "row_count", "row_format", "row_number", "rtree", "savepoint", "schedule", "schema", "schemas", "schema_name", "second", "secondary_engine", "secondary_load", "secondary_unload", "second_microsecond", "security", "select", "sensitive", "separator", "serial", "serializable", "server", "session", "set", "share", "show", "shutdown", "signal", "signed", "simple", "skip", "slave", "slow", "smallint", "snapshot", "socket", "some", "soname", "sounds", "source", "spatial", "specific", "sql", "sqlexception", "sqlstate", "sqlwarning", "sql_after_gtids", "sql_after_mts_gaps", "sql_before_gtids", "sql_big_result", "sql_buffer_result", "sql_cache", "sql_calc_found_rows", "sql_no_cache", "sql_small_result", "sql_thread", "sql_tsi_day", "sql_tsi_hour", "sql_tsi_minute", "sql_tsi_month", "sql_tsi_quarter", "sql_tsi_second", "sql_tsi_week", "sql_tsi_year", "srid", "ssl", "stacked", "start", "starting", "starts", "stats_auto_recalc", "stats_persistent", "stats_sample_pages", "status", "stop", "storage", "stored", "straight_join", "string", "subclass_origin", "subject", "subpartition", "subpartitions", "super", "suspend", "swaps", "switches", "system", "table", "tables", "tablespace", "table_checksum", "table_name", "temporary", "temptable", "terminated", "text", "than", "then", "thread_priority", "ties", "time", "timestamp", "timestampadd", "timestampdiff", "tinyblob", "tinyint", "tinytext", "to", "trailing", "transaction", "trigger", "triggers", "true", "truncate", "type", "types", "unbounded", "uncommitted", "undefined", "undo", "undofile", "undo_buffer_size", "unicode", "uninstall", "union", "unique", "unknown", "unlock", "unsigned", "until", "update", "upgrade", "usage", "use", "user", "user_resources", "use_frm", "using", "utc_date", "utc_time", "utc_timestamp", "validation", "value", "values", "varbinary", "varchar", "varcharacter", "variables", "varying", "vcpu", "view", "virtual", "visible", "wait", "warnings", "week", "weight_string", "when", "where", "while", "window", "with", "without", "work", "wrapper", "write", "x509", "xa", "xid", "xml", "xor", "year", "year_month", "zerofill", "active", "admin", "buckets", "clone", "component", "cume_dist", "definition", "dense_rank", "description", "empty", "except", "exclude", "first_value", "following", "geomcollection", "get_master_public_key", "grouping", "groups", "histogram", "history", "inactive", "invisible", "json_table", "lag", "last_value", "lateral", "lead", "locked", "master_public_key_path", "nested", "nowait", "nth_value", "ntile", "nulls", "of", "old", "optional", "ordinality", "organization", "others", "over", "path", "percent_rank", "persist", "persist_only", "preceding", "process", "rank", "recursive", "reference", "resource", "respect", "restart", "retain", "reuse", "role", "row_number", "secondary_engine", "secondary_load", "secondary_unload", "skip", "srid", "system", "thread_priority", "ties", "unbounded", "vcpu", "visible", "window", "analyse", "des_key_file", "parse_gcol_expr", "redofile", "sql_cache"}

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
		ks = append(ks, " `"+k+"`"+kt)
		vs = append(vs, v)
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
		fieldVal := v.Field(i).Interface()
		if fieldVal == "" {
			if tags[i] == "id" ||
				table.Columns[tags[i]].DataType == "datetime" {
				continue
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

// MapsToCRUDRows convert []map[string]string to crud.RowsMap
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

// WhereTimeParse 将时间段转换成对应SQL
func WhereTimeParse(field, ts string, years, months, days int) string {
	// (createdtime >= '2018-01-01 00:00:00' AND createdtime < '2018-01-02 00:00:00')
	var a, b, format string
	format = "2006-01-02 15:04:05"[:len(ts)]
	t, _ := time.ParseInLocation(format, ts, time.Local)
	a = t.Format("2006-01-02 15:04:05")
	b = t.AddDate(years, months, days).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("(%s >= '%s' AND %s < '%s')", field, a, field, b)
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
	case int64:
		i = int(v)
	// 不实现除了uint64之后的无符号
	case uint64:
		i = int(v)
	case int:
		i = v
	case int8:
		i = int(v)
	case int16:
		i = int(v)
	case int32:
		i = int(v)
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
