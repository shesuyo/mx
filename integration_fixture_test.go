package mx

import (
	_ "embed"
	"fmt"
	"os"
	"strings"
)

//go:embed testdata/integration_fixture.sql
var integrationFixtureSQL string

func prepareIntegrationFixture(db *DataBase) error {
	if db == nil || db.DB() == nil {
		return fmt.Errorf("integration fixture database is nil")
	}
	if !allowIntegrationFixtureReset(db.Schema) {
		return fmt.Errorf("refuse to reset schema %q; use a test database or set MX_FIXTURE_RESET=1", db.Schema)
	}

	if _, err := db.DB().Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("disable foreign key checks: %w", err)
	}
	defer func() {
		_, _ = db.DB().Exec("SET FOREIGN_KEY_CHECKS = 1")
	}()

	for _, query := range splitIntegrationFixtureSQL(integrationFixtureSQL) {
		if _, err := db.DB().Exec(query); err != nil {
			return fmt.Errorf("prepare integration fixture: %w; sql: %s", err, query)
		}
	}
	return nil
}

func splitIntegrationFixtureSQL(sql string) []string {
	parts := strings.Split(sql, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		statement := strings.TrimSpace(part)
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

func allowIntegrationFixtureReset(schema string) bool {
	if os.Getenv("MX_FIXTURE_RESET") == "1" {
		return true
	}
	return strings.Contains(strings.ToLower(schema), "test")
}
