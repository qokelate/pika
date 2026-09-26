package database

import (
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestSettingsForDriver(t *testing.T) {
	postgres := settingsForDriver("postgres")
	if postgres.MaxOpenConns != 64 || postgres.MaxIdleConns != 16 {
		t.Fatalf("postgres pool = %+v, want max open 64 and max idle 16", postgres)
	}
	if postgres.ConnMaxLifetime != 30*time.Minute || postgres.ConnMaxIdleTime != 5*time.Minute {
		t.Fatalf("postgres connection lifetime settings = %+v", postgres)
	}

	sqlite := settingsForDriver("sqlite")
	if sqlite.MaxOpenConns != 1 || sqlite.MaxIdleConns != 1 {
		t.Fatalf("sqlite pool = %+v, want a single connection", sqlite)
	}
}

func TestSettingsForPostgresAliasesUseBoundedPool(t *testing.T) {
	for _, driver := range []string{"postgres", "postgresql", "unknown"} {
		settings := settingsForDriver(driver)
		if settings.MaxOpenConns <= 0 || settings.MaxIdleConns <= 0 || settings.MaxIdleConns > settings.MaxOpenConns {
			t.Fatalf("driver %q has invalid pool settings: %+v", driver, settings)
		}
	}
}

func TestConfigureConnectionPoolAppliesSettings(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}

	if err := ConfigureConnectionPool(db, nil); err != nil {
		t.Fatalf("configure connection pool: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql database: %v", err)
	}
	defer sqlDB.Close()

	stats := sqlDB.Stats()
	if stats.MaxOpenConnections != sqliteMaxOpenConns {
		t.Fatalf("max open connections = %d, want %d", stats.MaxOpenConnections, sqliteMaxOpenConns)
	}
}
