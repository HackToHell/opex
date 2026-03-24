// Package migrations provides embedded database schema migrations for opex.
// Migrations are embedded in the binary and run on startup using
// golang-migrate/migrate with the ClickHouse driver.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	// The clickhouse-go/v2 package registers a database/sql driver named
	// "clickhouse" in its init(). It is already imported by the main
	// clickhouse client package, but we import it explicitly here so this
	// package can be used independently.
	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/golang-migrate/migrate/v4"
	mch "github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/hacktohell/opex/internal/config"
)

//go:embed sql/*.sql
var migrationFS embed.FS

type templateData struct {
	DatabaseNames         []string
	TracesTable           string
	TraceMetadataTable    string
	TraceMetadataView     string
	SpanTagNamesTable     string
	SpanTagNamesView      string
	ResourceTagNamesTable string
	ResourceTagNamesView  string
	ServiceNamesTable     string
	ServiceNamesView      string
}

// Run opens a temporary database/sql connection using the provided ClickHouse
// config, renders the embedded migration templates using the configured table
// names, runs all pending schema migrations, then closes the connection.
//
// All migration SQL uses idempotent DDL (CREATE ... IF NOT EXISTS) so multiple
// replicas can safely run migrations concurrently without distributed locking.
func Run(cfg config.ClickHouseConfig, logger *slog.Logger) error {
	renderedFS, cleanup, err := renderMigrations(cfg)
	if err != nil {
		return fmt.Errorf("rendering migrations: %w", err)
	}
	defer cleanup()

	source, err := iofs.New(renderedFS, ".")
	if err != nil {
		return fmt.Errorf("creating migration source: %w", err)
	}

	// Open a database/sql connection for migrations only. The clickhouse-go/v2
	// package registers itself as a database/sql driver under the name
	// "clickhouse" and accepts clickhouse:// DSNs directly.
	db, err := sql.Open("clickhouse", cfg.DSN)
	if err != nil {
		return fmt.Errorf("opening migration db connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("pinging migration db: %w", err)
	}

	driver, err := mch.WithInstance(db, &mch.Config{
		MigrationsTableEngine: "MergeTree",
		MultiStatementEnabled: true,
	})
	if err != nil {
		db.Close()
		return fmt.Errorf("creating migration driver: %w", err)
	}
	// After WithInstance succeeds, the driver owns the *sql.DB. On error
	// paths below, close via the driver (or via m.Close()) rather than
	// calling db.Close() directly to avoid double-close.

	m, err := migrate.NewWithInstance("iofs", source, "clickhouse", driver)
	if err != nil {
		driver.Close()
		return fmt.Errorf("creating migrate instance: %w", err)
	}

	logger.Info("running database migrations")

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		m.Close()
		return fmt.Errorf("applying migrations: %w", err)
	} else if err == migrate.ErrNoChange {
		logger.Info("database schema is up to date")
	} else {
		v, dirty, verr := m.Version()
		if verr != nil {
			logger.Warn("could not read migration version after apply", "error", verr)
		} else {
			logger.Info("migrations applied successfully", "version", v, "dirty", dirty)
		}
	}

	// m.Close() closes source, driver, and the underlying *sql.DB connection.
	srcErr, dbErr := m.Close()
	if srcErr != nil {
		return fmt.Errorf("closing migration source: %w", srcErr)
	}
	if dbErr != nil {
		return fmt.Errorf("closing migration driver: %w", dbErr)
	}

	return nil
}

func renderMigrations(cfg config.ClickHouseConfig) (fs.FS, func(), error) {
	tempDir, err := os.MkdirTemp("", "opex-migrations-*")
	if err != nil {
		return nil, nil, fmt.Errorf("creating temp migration dir: %w", err)
	}

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	data := templateData{
		DatabaseNames:         databaseNames(cfg),
		TracesTable:           cfg.TracesTable,
		TraceMetadataTable:    cfg.TraceMetadataTable,
		TraceMetadataView:     materializedViewName(cfg.TraceMetadataTable),
		SpanTagNamesTable:     cfg.SpanTagNamesTable,
		SpanTagNamesView:      materializedViewName(cfg.SpanTagNamesTable),
		ResourceTagNamesTable: cfg.ResourceTagNamesTable,
		ResourceTagNamesView:  materializedViewName(cfg.ResourceTagNamesTable),
		ServiceNamesTable:     cfg.ServiceNamesTable,
		ServiceNamesView:      materializedViewName(cfg.ServiceNamesTable),
	}

	entries, err := migrationFS.ReadDir("sql")
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("reading embedded migrations: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join("sql", entry.Name())
		content, err := fs.ReadFile(migrationFS, path)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("reading embedded migration %s: %w", entry.Name(), err)
		}

		tmpl, err := template.New(entry.Name()).Option("missingkey=error").Parse(string(content))
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("parsing migration template %s: %w", entry.Name(), err)
		}

		outPath := filepath.Join(tempDir, entry.Name())
		outFile, err := os.Create(outPath)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("creating rendered migration %s: %w", entry.Name(), err)
		}

		if err := tmpl.Execute(outFile, data); err != nil {
			_ = outFile.Close()
			cleanup()
			return nil, nil, fmt.Errorf("rendering migration %s: %w", entry.Name(), err)
		}

		if err := outFile.Close(); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("closing rendered migration %s: %w", entry.Name(), err)
		}
	}

	return os.DirFS(tempDir), cleanup, nil
}

func databaseNames(cfg config.ClickHouseConfig) []string {
	names := make(map[string]struct{})
	for _, table := range []string{
		cfg.TracesTable,
		cfg.TraceMetadataTable,
		cfg.SpanTagNamesTable,
		cfg.ResourceTagNamesTable,
		cfg.ServiceNamesTable,
	} {
		database, _ := splitQualifiedName(table)
		if database != "" {
			names[database] = struct{}{}
		}
	}

	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func materializedViewName(table string) string {
	database, name := splitQualifiedName(table)
	viewName := name + "_mv"
	if database == "" {
		return viewName
	}
	return database + "." + viewName
}

func splitQualifiedName(name string) (string, string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}
