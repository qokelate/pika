package database

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	postgresMaxOpenConns    = 64
	postgresMaxIdleConns    = 16
	postgresConnMaxLifetime = 30 * time.Minute
	postgresConnMaxIdleTime = 5 * time.Minute

	sqliteMaxOpenConns    = 1
	sqliteMaxIdleConns    = 1
	sqliteConnMaxLifetime = 0
	sqliteConnMaxIdleTime = 0
)

type PoolSettings struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func ConfigureConnectionPool(db *gorm.DB, logger *zap.Logger) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get database connection pool: %w", err)
	}

	settings := settingsForDriver(db.Dialector.Name())
	sqlDB.SetMaxOpenConns(settings.MaxOpenConns)
	sqlDB.SetMaxIdleConns(settings.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(settings.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(settings.ConnMaxIdleTime)

	if logger != nil {
		logger.Info("database connection pool configured",
			zap.String("driver", db.Dialector.Name()),
			zap.Int("maxOpenConns", settings.MaxOpenConns),
			zap.Int("maxIdleConns", settings.MaxIdleConns),
			zap.Duration("connMaxLifetime", settings.ConnMaxLifetime),
			zap.Duration("connMaxIdleTime", settings.ConnMaxIdleTime))
	}

	return nil
}

func settingsForDriver(driver string) PoolSettings {
	switch strings.ToLower(driver) {
	case "sqlite":
		return PoolSettings{
			MaxOpenConns:    sqliteMaxOpenConns,
			MaxIdleConns:    sqliteMaxIdleConns,
			ConnMaxLifetime: sqliteConnMaxLifetime,
			ConnMaxIdleTime: sqliteConnMaxIdleTime,
		}
	default:
		return PoolSettings{
			MaxOpenConns:    postgresMaxOpenConns,
			MaxIdleConns:    postgresMaxIdleConns,
			ConnMaxLifetime: postgresConnMaxLifetime,
			ConnMaxIdleTime: postgresConnMaxIdleTime,
		}
	}
}
