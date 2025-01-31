package triggers

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
	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/obot-platform/nah/pkg/logrus"
	"github.com/obot-platform/nah/pkg/triggers/statements"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func newDBStore(dsn string) (store, error) {
	db, err := newDB(dsn)
	if err != nil {
		return nil, err
	}

	statements := statements.New()
	_, err = db.Exec(statements.CreateRevisionsTable())
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return &dbStore{
		db:         db,
		statements: statements,
		cache:      make(map[string]int64),
	}, nil
}

type dbStore struct {
	lock       sync.RWMutex
	db         *sql.DB
	statements *statements.Statements
	cache      map[string]int64
}

func (d *dbStore) Close() error {
	return d.db.Close()
}

func (d *dbStore) Trigger(ctx context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) (err error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	if shouldTrigger, err := d.shouldTrigger(ctx, tx, gvk, obj); err != nil || !shouldTrigger {
		return err
	}

	if err := d.createMatchersTable(ctx, tx, gvk.Kind); err != nil {
		return fmt.Errorf("failed to create matchers table for %s: %w", gvk.Kind, err)
	}

	return d.trigger(ctx, tx, trigger, gvk, key, namespace, name, obj)
}

func (d *dbStore) RegisterMatcher(ctx context.Context, gvk schema.GroupVersionKind, key string, sourceGVK schema.GroupVersionKind, matcher objectMatcher) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	if err := d.createMatchersTable(ctx, tx, sourceGVK.Kind); err != nil {
		return fmt.Errorf("failed to create matchers table for %s: %w", sourceGVK.Kind, err)
	}

	var labelSelector, fieldSelector string
	if matcher.Selector != nil {
		labelSelector = matcher.Selector.String()
	}
	if matcher.Fields != nil {
		fieldSelector = matcher.Fields.String()
	}

	apiVersion, kind := gvk.ToAPIVersionAndKind()

	_, err = tx.ExecContext(ctx, d.statements.InsertMatcher(sourceGVK.Kind), apiVersion, kind, key, matcher.Namespace, matcher.Name, labelSelector, fieldSelector)
	return err
}

func (d *dbStore) UnregisterMatcher(ctx context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	var errs []error

	if shouldTrigger, err := d.shouldTrigger(ctx, tx, gvk, obj); err == nil && shouldTrigger {
		// Even if there is an error during triggering, try to clean everything up.
		// We won't trigger on this again after it is removed.
		if triggerErr := d.trigger(ctx, tx, trigger, gvk, key, namespace, name, obj); triggerErr != nil {
			errs = append(errs, triggerErr)
		}
	} else if err != nil {
		errs = append(errs, err)
	}

	// The cleanup is really all best-effort. The object is getting deleted, so we won't trigger on it again unless it is recreated.
	apiVersion, kind := gvk.ToAPIVersionAndKind()
	// Delete all triggers from this object.
	for _, statement := range d.statements.DeleteMatchers() {
		if _, err = tx.ExecContext(ctx, statement, apiVersion, kind, key); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete matchers for %s: %w", gvk.Kind, err))
		}
	}

	if _, err = tx.ExecContext(ctx, d.statements.DeleteExactMatchers(gvk.Kind), namespace, name); err != nil {
		errs = append(errs, fmt.Errorf("failed to delete exact matchers for %s: %w", gvk.Kind, err))
	}

	return errors.Join(errs...)
}

func (d *dbStore) trigger(ctx context.Context, tx *sql.Tx, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) error {
	rows, err := tx.QueryContext(ctx, d.statements.GetMatchers(gvk.Kind), namespace, name)
	if err != nil {
		return fmt.Errorf("failed to get matchers for %s: %w", gvk.Kind, err)
	}
	defer rows.Close()

	var (
		targetAPIVersion, targetKind, targetKey                           string
		matcherNamespace, matcherName, labelSelectorStr, fieldSelectorStr *string
		matcher                                                           objectMatcher
	)
	for rows.Next() {
		if err = rows.Scan(&targetAPIVersion, &targetKind, &targetKey, &matcherNamespace, &matcherName, &labelSelectorStr, &fieldSelectorStr); err != nil {
			return fmt.Errorf("failed to scan matchers for %s: %w", gvk.Kind, err)
		}

		if labelSelectorStr != nil {
			matcher.Selector, err = labels.Parse(*labelSelectorStr)
			if err != nil {
				return fmt.Errorf("failed to parse label selector %q for %s: %w", *labelSelectorStr, gvk.Kind, err)
			}
		} else {
			matcher.Selector = labels.Everything()
		}

		if fieldSelectorStr != nil {
			matcher.Fields, err = fields.ParseSelector(*fieldSelectorStr)
			if err != nil {
				return fmt.Errorf("failed to parse field selector %q for %s: %w", *fieldSelectorStr, gvk.Kind, err)
			}
		} else {
			matcher.Fields = fields.Everything()
		}

		if matcherNamespace != nil {
			matcher.Namespace = *matcherNamespace
		} else {
			matcher.Namespace = ""
		}
		if matcherName != nil {
			matcher.Name = *matcherName
		} else {
			matcher.Name = ""
		}

		targetGVK := schema.FromAPIVersionAndKind(targetAPIVersion, targetKind)

		if matcher.match(namespace, name, obj) {
			log.Debugf("Triggering [%s] [%v] from [%s] [%v]", targetKey, targetGVK, key, gvk)
			if err = trigger.Trigger(targetGVK, targetKey, 0); err != nil {
				log.Errorf("failed to trigger %s: %v", targetKey, err)
			}
		}
	}

	if err = rows.Err(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	return nil
}

func (d *dbStore) shouldTrigger(ctx context.Context, tx *sql.Tx, gvk schema.GroupVersionKind, obj kclient.Object) (bool, error) {
	if obj == nil {
		return true, nil
	}
	updated, err := d.storeRevision(ctx, tx, gvk, obj.GetResourceVersion())
	if err != nil {
		return false, fmt.Errorf("failed to store revision: %w", err)
	}
	return updated, nil
}

func (d *dbStore) createMatchersTable(ctx context.Context, tx *sql.Tx, kind string) error {
	s := d.statements.CreateMatchersTable(kind)
	if s == "" {
		// Trigger table already created
		return nil
	}

	_, err := tx.ExecContext(ctx, s)
	return err
}

func (d *dbStore) storeRevision(ctx context.Context, tx *sql.Tx, gvk schema.GroupVersionKind, revision string) (bool, error) {
	rev, err := strconv.ParseInt(revision, 10, 64)
	if err != nil {
		return false, nil
	}

	kind := gvk.Kind

	d.lock.RLock()
	latest := d.cache[kind]
	d.lock.RUnlock()
	if latest != 0 && rev <= latest {
		return false, nil
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	if latest = d.cache[kind]; latest != 0 && rev <= latest {
		return false, nil
	}

	if err := tx.QueryRowContext(ctx, d.statements.GetLatestRevision(), kind).Scan(&latest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("error getting latest revision: %v", err)
	}

	if latest != 0 && rev <= latest {
		d.cache[kind] = latest
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, d.statements.UpdateLatestRevision(), kind, revision); err != nil {
		return false, fmt.Errorf("failed to update latest revision for %q: %w", kind, err)
	}

	d.cache[kind] = rev

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
