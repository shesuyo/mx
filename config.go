package crud

// config var
var (
	DefaultMaxIdleConns = 10
	DefaultMaxOpenConns = 10

	DefaultConfig = Config{}
)

// Config 用于创建连接的配置配置
type Config struct {
	DataSourceName string
	MaxIdleConns   int
	MaxOpenConns   int
}

func (config *Config) parse() {

}
