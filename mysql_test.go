package mx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestMySQLDSNWithDefaults(t *testing.T) {
	// 用非法布尔值检测解析器是否识别此选项，独立验证能力检测结果。
	_, err := mysql.ParseDSN("/?tinyInt1IsBool=invalid")
	supported := err != nil
	if mysqlSupportsTinyInt1IsBool != supported {
		t.Fatalf("tinyInt1IsBool support = %v, want %v", mysqlSupportsTinyInt1IsBool, supported)
	}
	t.Logf("driver supports tinyInt1IsBool: %v", supported)

	tests := []struct {
		name string
		dsn  string
		want string // 支持该参数时的结果；旧驱动应原样返回。
	}{
		{"empty", "", "/?tinyInt1IsBool=false"},
		{"no database", "/", "/?tinyInt1IsBool=false"},
		{"database", "/demo", "/demo?tinyInt1IsBool=false"},
		{"tcp", "user:password@tcp(localhost:3306)/demo", "user:password@tcp(localhost:3306)/demo?tinyInt1IsBool=false"},
		{"unix", "user:password@unix(/tmp/mysql.sock)/demo", "user:password@unix(/tmp/mysql.sock)/demo?tinyInt1IsBool=false"},
		{"empty query", "/demo?", "/demo?&tinyInt1IsBool=false"},
		{"parameters", "/demo?charset=utf8mb4&parseTime=true", "/demo?charset=utf8mb4&parseTime=true&tinyInt1IsBool=false"},
		{"parameter order", "/demo?@b=2&@a=%40b", "/demo?@b=2&@a=%40b&tinyInt1IsBool=false"},
		{"password contains option", "user:p?tinyInt1IsBool=true@tcp(localhost)/demo", "user:p?tinyInt1IsBool=true@tcp(localhost)/demo?tinyInt1IsBool=false"},
		{"password contains slash", "user:p/word@tcp(localhost)/demo?parseTime=true", "user:p/word@tcp(localhost)/demo?parseTime=true&tinyInt1IsBool=false"},
		{"explicit true", "/demo?tinyInt1IsBool=true", "/demo?tinyInt1IsBool=true"},
		{"explicit false", "/demo?tinyInt1IsBool=false", "/demo?tinyInt1IsBool=false"},
		{"explicit numeric", "/demo?parseTime=true&tinyInt1IsBool=1", "/demo?parseTime=true&tinyInt1IsBool=1"},
		{"explicit repeated", "/demo?tinyInt1IsBool=false&tinyInt1IsBool=true", "/demo?tinyInt1IsBool=false&tinyInt1IsBool=true"},
		{"invalid dsn", "bad mysql dsn", "bad mysql dsn"},
		{"invalid value", "/demo?tinyInt1IsBool=invalid", "/demo?tinyInt1IsBool=invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := tt.want
			if !supported {
				want = tt.dsn
			}
			got := mysqlDSNWithDefaults(tt.dsn)
			if got != want {
				t.Fatalf("mysqlDSNWithDefaults(%q) = %q, want %q", tt.dsn, got, want)
			}
			if again := mysqlDSNWithDefaults(got); again != got {
				t.Fatalf("repeated defaults changed DSN: %q => %q", got, again)
			}
		})
	}
}

func TestMySQLDSNWithDefaultsPreservesConfig(t *testing.T) {
	dsn := "user:p?/word@tcp(localhost:3306)/demo?charset=utf8mb4&parseTime=true&timeout=2s&loc=UTC&time_zone=%27%2B08%3A00%27"
	original, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	got, err := mysql.ParseDSN(mysqlDSNWithDefaults(dsn))
	if err != nil {
		t.Fatal(err)
	}
	if _, unknown := got.Params["tinyInt1IsBool"]; unknown {
		t.Fatal("tinyInt1IsBool must not be sent as a MySQL session variable")
	}
	if mysqlSupportsTinyInt1IsBool {
		if !strings.Contains(got.FormatDSN(), "tinyInt1IsBool=false") {
			t.Fatal("driver did not preserve numeric TINYINT behavior")
		}
		// 用驱动重新解析 true，检查添加默认值没有改变其他配置。
		got, err = mysql.ParseDSN(mysqlDSNWithDefaults(dsn) + "&tinyInt1IsBool=true")
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatal("adding the default changed other MySQL configuration")
	}
}
