package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/obot-platform/nah/pkg/logrus"
	"github.com/obot-platform/nah/pkg/persistence/statements"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Store interface {
	Store(context.Context, schema.GroupVersionKind, string) (bool, error)
}

func NewNoOp() Store {
	return new(none)
}

type none struct{}

func (n none) Store(context.Context, schema.GroupVersionKind, string) (bool, error) {
	return false, nil
}

func NewDBStore(dsn string) (Store, error) {
	db, err := newDB(dsn)
	if err != nil {
		return nil, err
	}

	statements := statements.New()
	_, err = db.Exec(statements.CreateTable())
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return &dbPersistentTriggerStore{
		db:         db,
		statements: statements,
		cache:      make(map[string]int64),
	}, nil
}

type dbPersistentTriggerStore struct {
	lock       sync.RWMutex
	db         *sql.DB
	statements *statements.Statements
	cache      map[string]int64
}

func (s *dbPersistentTriggerStore) Store(ctx context.Context, gvk schema.GroupVersionKind, revision string) (bool, error) {
	if s == nil {
		return false, nil
	}

	rev, err := strconv.ParseInt(revision, 10, 64)
	if err != nil {
		return false, nil
	}

	gvkStr := gvk.String()

	s.lock.RLock()
	latest := s.cache[gvkStr]
	s.lock.RUnlock()
	if latest != 0 && rev <= latest {
		return false, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	if latest = s.cache[gvkStr]; latest != 0 && rev <= latest {
		return false, nil
	}

	if err := s.db.QueryRowContext(ctx, s.statements.GetLatestRevision(), gvkStr).Scan(&latest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("error getting latest revision: %v", err)
	}

	if latest != 0 && rev <= latest {
		s.cache[gvkStr] = latest
		return false, nil
	}

	if _, err := s.db.ExecContext(ctx, s.statements.UpdateLatestRevision(), gvkStr, revision); err != nil {
		return false, fmt.Errorf("failed to update latest revision for %q: %w", gvkStr, err)
	}

	s.cache[gvkStr] = rev

	return true, nil
}

func newDB(dsn string) (*sql.DB, error) {
	var (
		gdb                    gorm.Dialector
		pool                   bool
		skipDefaultTransaction bool
	)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	case strings.HasPrefix(dsn, "postgresql://"):
		dsn = strings.Replace(dsn, "postgresql://", "postgres://", 1)
		fallthrough
	case strings.HasPrefix(dsn, "postgres://"):
		gdb = postgres.Open(dsn)
		pool = true
	default:
		return nil, fmt.Errorf("unsupported database: %s", dsn)
	}
	db, err := gorm.Open(gdb, &gorm.Config{
		SkipDefaultTransaction: skipDefaultTransaction,
		Logger: logrus.New(logrus.Config{
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: true,
			LogSQL:                    true,
		}),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(time.Minute * 3)
	if pool {
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetMaxOpenConns(5)
	} else {
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetMaxOpenConns(1)
	}

	return sqlDB, nil
}
