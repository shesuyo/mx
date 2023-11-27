package mx

import (
	"time"
)

// config var
var (
	DefaultMaxIdleConns = 10
	DefaultMaxOpenConns = 10

	DefaultConfig = Config{
		MaxIdleConns: DefaultMaxIdleConns,
		MaxOpenConns: DefaultMaxOpenConns,
		Timeout:      2 * time.Second,
	}
)

// Config 用于创建连接的配置配置
type Config struct {
	DataSourceName string
	MaxIdleConns   int
	MaxOpenConns   int
	Log            Logger
	Timeout        time.Duration
}

type Logger interface {
	LogSql(LogSqlData)
	ErrSql(LogSqlData)
}

type LogSqlData struct {
	Sql      string        `json:"sql"`
	Duration time.Duration `json:"duration"`
	Callers  LogSqlCallers `json:"callers"`
	Way      LogSqlWay     `json:"way"` // Query Exec
	Err      error         `json:"err"`
}

type LogSqlWay int

var (
	QueryWay LogSqlWay = 1
	ExecWay  LogSqlWay = 2
)

type LogSqlCaller struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type LogSqlCallers []LogSqlCaller

func (config *Config) parse() {

}
