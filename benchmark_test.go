package mx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	benchmarkStringSink string
	benchmarkBytesSink  []byte
	benchmarkIntSink    int
	benchmarkAnySink    any
)

// go test -run=^$ -bench "^(BenchmarkReflectFunc|BenchmarkAssertionFunc)$" -benchmem -count=5
// goos: windows
// goarch: amd64
// pkg: github.com/shesuyo/mx
// cpu: 13th Gen Intel(R) Core(TM) i7-13700K
// BenchmarkReflectFunc-24          4350504               281.6 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4272934               281.9 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4390539               278.7 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4406395               275.7 ns/op           136 B/op          5 allocs/op
// BenchmarkReflectFunc-24          4313017               278.6 ns/op           136 B/op          5 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8620 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8652 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8759 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8607 ns/op          0 B/op          0 allocs/op
// BenchmarkAssertionFunc-24     1000000000                 0.8632 ns/op          0 B/op          0 allocs/op
// avg: reflect 279.3 ns/op, assertion 0.8654 ns/op
// PASS
// ok      github.com/shesuyo/mx   11.051s

func BenchmarkReflectFunc(b *testing.B) {
	u := reflect.ValueOf(&User{})

	for b.Loop() {
		if af := u.MethodByName(AfterFind); af.IsValid() {
			af.Call(nil)
		}
	}
}

func BenchmarkAssertionFunc(b *testing.B) {
	var u any = &User{}

	for b.Loop() {
		if af, ok := u.(AfterFinder); ok {
			af.AfterFind()
		}
	}
}

// go test -benchmem -bench "^(BenchmarkMapGet)|(BenchmarkSliceGet.{1,2})$"
// goos: windows
// goarch: amd64
// pkg: github.com/shesuyo/mx
// BenchmarkMapGet-16              100000000               11.1 ns/op             0 B/op          0 allocs/op
// BenchmarkSliceGet0-16           2000000000               0.84 ns/op            0 B/op          0 allocs/op
// BenchmarkSliceGet10-16          300000000                5.05 ns/op            0 B/op          0 allocs/op
// BenchmarkSliceGet19-16          100000000               11.8 ns/op             0 B/op          0 allocs/op
// PASS
// ok      github.com/shesuyo/mx   6.167s

func BenchmarkMapGet(b *testing.B) {
	m := make(map[string]Columns)
	for i := range 20 {
		m["field"+strconv.Itoa(i)] = Columns{}
	}

	for b.Loop() {
		_ = m["field19"]
	}
}

type KeyWithColumns struct {
	key  string
	cols Columns
}

func BenchmarkSliceGet0(b *testing.B) {
	m := []KeyWithColumns{}
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}

	for b.Loop() {
		for j := range 20 {
			if m[j].key == "field0" {
				break
			}
		}
	}
}

func BenchmarkSliceGet10(b *testing.B) {
	m := []KeyWithColumns{}
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 20 {
			if m[j].key == "field10" {
				break
			}
		}
	}
}
func BenchmarkSliceGet19(b *testing.B) {
	m := []KeyWithColumns{}
	for i := range 20 {
		m = append(m, KeyWithColumns{key: "field" + strconv.Itoa(i), cols: Columns{}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 20 {
			if m[j].key == "field19" {
				break
			}
		}
	}
}

func BenchmarkClone(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		_ = UserTable.Clone()
	}
}

// 1s 1_000ms 1_000_000us 1_000_000_000ns

// BenchmarkQuery-16    	   10000	    109262 ns/op	    1624 B/op	      41 allocs/op
func BenchmarkQuery(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		sr := UserTable.Query("SELECT * FROM user WHERE id = ?", 2)
		for sr.rows.Next() {
			vals := make([]*sql.RawBytes, 7)
			for i := range 7 {
				vals[i] = &sql.RawBytes{}
			}
			sr.Scan(&vals)
		}
	}
}

// BenchmarkReflectAStruct-16    	    3122	    383770 ns/op	   15140 B/op	     389 allocs/op 加载一个结构体和一个slice
// BenchmarkReflectAStruct-16    	    9615	    117831 ns/op	    3676 B/op	      98 allocs/op
func BenchmarkReflectAStruct(b *testing.B) {
	requireIntegrationUserTable(b)
	for i := 0; i < b.N; i++ {
		_ = UserTable.Where("id = ?", 2).ToStruct(&User{})
	}
}

// has one
// has many
// many to many

const (
	queryPathBenchDriverName = "mx_query_path_bench_stub"
	queryPathBenchTableName  = "query_path_bench"
	queryPathBenchRowCount   = 128
)

var (
	queryPathBenchColumns = []string{
		"id",
		"name",
		"age",
		"uid",
		"phone",
		"amount",
		"status",
		"ctime",
	}
	queryPathBenchRows [][]driver.Value
	queryPathBenchOnce sync.Once
)

func init() {
	sql.Register(queryPathBenchDriverName, queryPathBenchDriver{})
}

type queryPathBenchDriver struct{}

func (queryPathBenchDriver) Open(name string) (driver.Conn, error) {
	return queryPathBenchConn{}, nil
}

type queryPathBenchConn struct{}

func (queryPathBenchConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (queryPathBenchConn) Close() error { return nil }

func (queryPathBenchConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (queryPathBenchConn) Ping(ctx context.Context) error { return nil }

func (queryPathBenchConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryPathBenchOnce.Do(initQueryPathBenchRows)
	rows := queryPathBenchRows
	if strings.Contains(strings.ToLower(query), "limit") && len(rows) > 0 {
		rows = rows[:1]
	}
	return &queryPathBenchRowsStub{
		cols: queryPathBenchColumns,
		rows: rows,
	}, nil
}

type queryPathBenchRowsStub struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *queryPathBenchRowsStub) Columns() []string { return r.cols }
func (r *queryPathBenchRowsStub) Close() error      { return nil }
func (r *queryPathBenchRowsStub) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func initQueryPathBenchRows() {
	queryPathBenchRows = make([][]driver.Value, queryPathBenchRowCount)
	for i := range queryPathBenchRows {
		queryPathBenchRows[i] = []driver.Value{
			[]byte(strconv.Itoa(i + 1)),
			[]byte("name_" + strconv.Itoa(i+1)),
			[]byte(strconv.Itoa(20 + i%50)),
			[]byte(strconv.Itoa(1000 + i)),
			[]byte("1380000" + strconv.Itoa(1000+i)),
			[]byte(strconv.FormatFloat(float64(i)*1.25, 'f', 2, 64)),
			[]byte(strconv.Itoa(i % 3)),
			[]byte("2026-06-30 12:00:00"),
		}
		if i%16 == 0 {
			queryPathBenchRows[i][4] = nil
		}
	}
}

type queryPathBenchModel struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	Age    int     `json:"age"`
	UID    int     `json:"uid"`
	Phone  string  `json:"phone"`
	Amount float64 `json:"amount"`
	Status int     `json:"status"`
	Ctime  string  `json:"ctime"`
}

func (queryPathBenchModel) DBName() string { return queryPathBenchTableName }

func newQueryPathBenchRawDB(b *testing.B) *sql.DB {
	b.Helper()
	raw, err := sql.Open(queryPathBenchDriverName, "")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := raw.Close(); err != nil {
			b.Fatal(err)
		}
	})
	return raw
}

func newQueryPathBenchDB(b *testing.B) *DataBase {
	b.Helper()
	raw := newQueryPathBenchRawDB(b)
	cols := make(Columns, len(queryPathBenchColumns))
	for _, col := range queryPathBenchColumns {
		cols[col] = Column{Name: col}
	}
	return &DataBase{
		Schema: "bench_schema",
		db:     raw,
		tableColumns: map[string]Columns{
			queryPathBenchTableName: cols,
		},
		mm: new(sync.RWMutex),
	}
}

func newQueryPathBenchRows(b *testing.B, raw *sql.DB, limitOne bool) *SQLRows {
	b.Helper()
	query := "SELECT * FROM " + queryPathBenchTableName
	if limitOne {
		query += " LIMIT 1"
	}
	rows, err := raw.Query(query)
	if err != nil {
		b.Fatal(err)
	}
	return &SQLRows{rows: rows}
}

func BenchmarkQueryPathStubRawScan(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := raw.Query("SELECT * FROM " + queryPathBenchTableName)
		if err != nil {
			b.Fatal(err)
		}
		var total int
		containers := make([]any, len(queryPathBenchColumns))
		for i := range containers {
			containers[i] = &sql.RawBytes{}
		}
		for rows.Next() {
			if err := rows.Scan(containers...); err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
			for _, container := range containers {
				total += len(*container.(*sql.RawBytes))
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		benchmarkIntSink = total
	}
}

func BenchmarkQueryPathStubSQLRowsRowMap(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = newQueryPathBenchRows(b, raw, true).RowMap()
	}
}

func BenchmarkQueryPathStubSQLRowsRowsMap(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = newQueryPathBenchRows(b, raw, false).RowsMap()
	}
}

func BenchmarkQueryPathStubSQLRowsRowsMapInterface(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = newQueryPathBenchRows(b, raw, false).RowsMapInterface()
	}
}

func BenchmarkQueryPathStubSQLRowsRowsMapNull(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = newQueryPathBenchRows(b, raw, false).RowsMapNull()
	}
}

func BenchmarkQueryPathStubSQLRowsDoubleSlice(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		cols, data := newQueryPathBenchRows(b, raw, false).DoubleSlice()
		benchmarkIntSink = len(cols) + len(data)
	}
}

func BenchmarkQueryPathStubSQLRowsTripleByte(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		cols, data := newQueryPathBenchRows(b, raw, false).TripleByte()
		benchmarkIntSink = len(cols) + len(data)
	}
}

func BenchmarkQueryPathStubSQLRowsToStruct(b *testing.B) {
	raw := newQueryPathBenchRawDB(b)
	b.ReportAllocs()
	for b.Loop() {
		var lis []queryPathBenchModel
		if err := newQueryPathBenchRows(b, raw, false).ToStruct(&lis); err != nil {
			b.Fatal(err)
		}
		benchmarkIntSink = len(lis)
	}
}

func BenchmarkQueryPathStubTableStruct(b *testing.B) {
	table := newQueryPathBenchDB(b).Table(queryPathBenchTableName)
	b.ReportAllocs()
	for b.Loop() {
		var lis []queryPathBenchModel
		if err := table.Struct(&lis); err != nil {
			b.Fatal(err)
		}
		benchmarkIntSink = len(lis)
	}
}

func BenchmarkIntegrationQueryPathRawScan(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := table.DataBase.DB().Query("SELECT * FROM user")
		if err != nil {
			b.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		containers := make([]any, len(cols))
		for i := range containers {
			containers[i] = &sql.RawBytes{}
		}
		count := 0
		for rows.Next() {
			if err := rows.Scan(containers...); err != nil {
				_ = rows.Close()
				b.Fatal(err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		benchmarkIntSink = count
	}
}

func BenchmarkIntegrationQueryPathRowMap(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = table.Limit(1).RowMap()
	}
}

func BenchmarkIntegrationQueryPathRowsMap(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = table.RowsMap()
	}
}

func BenchmarkIntegrationQueryPathRowsMapInterface(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = table.RowsMapInterface()
	}
}

func BenchmarkIntegrationQueryPathRowsMapNull(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkAnySink = table.RowsMapNull()
	}
}

func BenchmarkIntegrationQueryPathDoubleSlice(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		cols, data := table.DoubleSlice()
		benchmarkIntSink = len(cols) + len(data)
	}
}

func BenchmarkIntegrationQueryPathTripleByte(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		cols, data := table.Query("SELECT * FROM user").TripleByte()
		benchmarkIntSink = len(cols) + len(data)
	}
}

func BenchmarkIntegrationQueryPathStruct(b *testing.B) {
	table := requireIntegrationUserTable(b)
	b.ReportAllocs()
	for b.Loop() {
		var lis []User
		if err := table.Struct(&lis); err != nil {
			b.Fatal(err)
		}
		benchmarkIntSink = len(lis)
	}
}

func BenchmarkIsSlice(b *testing.B) {
	args := []any{}
	for i := 0; i < b.N; i++ {
		isSlice(args)
	}
}

// 运行命令：go test -run=^$ -bench "Benchmark(Unsafe|Safe)(ByteString|StringByte|RowsByteToString)$" -benchmem -count=5
// 本机环境 windows/amd64、i7-13700K、count=5 的平均结果：
//
//	UnsafeByteString        ~0.40 ns/op, 0 B/op, 0 allocs/op
//	SafeByteString         ~15.57 ns/op, 64 B/op, 1 alloc/op
//	UnsafeStringByte        ~1.17 ns/op, 0 B/op, 0 allocs/op
//	SafeStringByte         ~41.20 ns/op, 64 B/op, 1 alloc/op
//	UnsafeRowsByteToString  ~2.20 us/op, 0 B/op, 0 allocs/op
//	SafeRowsByteToString   ~36.70 us/op, 16 KB/op, 1024 allocs/op
func BenchmarkUnsafeByteString(b *testing.B) {
	raw := []byte("mx benchmark payload for unsafe byte slice to string conversion")
	b.ReportAllocs()
	for b.Loop() {
		benchmarkStringSink = byteString(raw)
	}
}

func BenchmarkSafeByteString(b *testing.B) {
	raw := []byte("mx benchmark payload for safe byte slice to string conversion")
	b.ReportAllocs()
	for b.Loop() {
		benchmarkStringSink = string(raw)
	}
}

func BenchmarkUnsafeStringByte(b *testing.B) {
	raw := "mx benchmark payload for unsafe string to byte slice conversion"
	b.ReportAllocs()
	for b.Loop() {
		benchmarkBytesSink = stringByte(raw)
	}
}

func BenchmarkSafeStringByte(b *testing.B) {
	raw := "mx benchmark payload for safe string to byte slice conversion"
	b.ReportAllocs()
	for b.Loop() {
		benchmarkBytesSink = []byte(raw)
	}
}

func BenchmarkUnsafeRowsByteToString(b *testing.B) {
	rawRows := make([][][]byte, 128)
	rows := make([][]string, len(rawRows))
	for i := range rawRows {
		rawRows[i] = make([][]byte, 8)
		rows[i] = make([]string, len(rawRows[i]))
		for j := range rawRows[i] {
			rawRows[i][j] = []byte("row_" + strconv.Itoa(i) + "_field_" + strconv.Itoa(j))
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		total := 0
		for i, rawRow := range rawRows {
			for j, raw := range rawRow {
				rows[i][j] = byteString(raw)
				total += len(rows[i][j])
			}
		}
		benchmarkIntSink = total
	}
}

func BenchmarkSafeRowsByteToString(b *testing.B) {
	rawRows := make([][][]byte, 128)
	rows := make([][]string, len(rawRows))
	for i := range rawRows {
		rawRows[i] = make([][]byte, 8)
		rows[i] = make([]string, len(rawRows[i]))
		for j := range rawRows[i] {
			rawRows[i][j] = []byte("row_" + strconv.Itoa(i) + "_field_" + strconv.Itoa(j))
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		total := 0
		for i, rawRow := range rawRows {
			for j, raw := range rawRow {
				rows[i][j] = string(raw)
				total += len(rows[i][j])
			}
		}
		benchmarkIntSink = total
	}
}
