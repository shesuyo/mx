package mx

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

var mysqlSupportsTinyInt1IsBool = func() bool {
	// 旧驱动将未知参数放入 Params，连接时会将其作为 MySQL 会话变量设置。
	// 这里只解析固定 DSN，不连接数据库，也不依赖新版本才有的 Go API。
	cfg, err := mysql.ParseDSN("/?tinyInt1IsBool=false")
	if err != nil {
		return false
	}
	_, unsupported := cfg.Params["tinyInt1IsBool"]
	return !unsupported
}()

func mysqlDSNWithDefaults(dsn string) string {
	if !mysqlSupportsTinyInt1IsBool {
		return dsn
	}
	if dsn == "" {
		return "/?tinyInt1IsBool=false"
	}

	// 与驱动一样从最后一个 '/' 后查找参数，避免误判密码或 Unix socket 路径。
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 {
		return dsn // 无效 DSN 交给驱动报错。
	}
	_, params, hasParams := strings.Cut(dsn[slash+1:], "?")
	if !hasParams {
		return dsn + "?tinyInt1IsBool=false"
	}
	for _, param := range strings.Split(params, "&") {
		if key, _, ok := strings.Cut(param, "="); ok && key == "tinyInt1IsBool" {
			return dsn // 保留调用方的显式配置及驱动对参数值的校验。
		}
	}
	return dsn + "&tinyInt1IsBool=false"
}
