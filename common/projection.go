package common

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/core"
)

const projectionCount = 100

// BaseProjection is a base struct for all projections and queries
type BaseProjection struct {
	*sql.DB
	store Store
	name  string
}

// NewBaseProjection creates a new BaseProjection
func NewBaseProjection(db *sql.DB, store Store, name string) (*BaseProjection, error) {
	if err := registerProjection(db, name); err != nil {
		return nil, err
	}

	bp := BaseProjection{
		db,
		store,
		name,
	}

	if err := bp.dropTableIfFirstRun(); err != nil {
		return nil, err
	}

	return &bp, nil
}

func registerProjection(db *sql.DB, name string) error {
	if err := createProjectionsTable(db); err != nil {
		return fmt.Errorf("failed to create projections table: %w", err)
	}

	if err := insertProjectionRecord(db, name); err != nil {
		return fmt.Errorf("failed to insert projection record: %w", err)
	}

	return nil
}

func createProjectionsTable(db *sql.DB) error {
	_, err := db.Exec(`create table if not exists projections (id VARCHAR PRIMARY KEY, last_handled_event_seq INTEGER);`)
	return err
}

func insertProjectionRecord(db *sql.DB, name string) error {
	_, err := db.Exec(`insert into projections (id, last_handled_event_seq) values (?, ?) on conflict do nothing;`, name, 0)
	return err
}

func (bp *BaseProjection) dropTableIfFirstRun() error {
	ok, err := bp.isFirstRun()
	if err != nil {
		return fmt.Errorf("failed to check if first run: %w", err)
	}
	if ok {
		if err := bp.dropTable(); err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
	}

	return nil
}

func (bp *BaseProjection) isFirstRun() (bool, error) {
	lastEvent, err := bp.getLastHandledEventSeq()
	if err != nil {
		return false, err
	}

	return lastEvent == 0, nil
}

func (bp *BaseProjection) getLastHandledEventSeq() (int, error) {
	var last int
	err := bp.QueryRow(`select last_handled_event_seq from projections where id = ?;`, bp.name).Scan(&last)
	return last, err
}

func (bp *BaseProjection) dropTable() error {
	_, err := bp.Exec(`drop table if exists ` + bp.name + `;`)
	return err
}

func (bp *BaseProjection) Fetch() (core.Iterator, error) {
	lastStart, err := bp.getLastHandledEventSeq()
	if err != nil {
		return nil, fmt.Errorf("failed to get last processed event seq: %w", err)
	}

	return bp.store.All(core.Version(lastStart+1), projectionCount)
}

func (bp *BaseProjection) Increment() error {
	_, err := bp.Exec(`update projections set last_handled_event_seq = last_handled_event_seq + 1 where id = ?;`, bp.name)
	return err
}

// Store is an interface that limits the methods that can be called on a store to All method only
type Store interface {
	All(start core.Version, count uint64) (core.Iterator, error)
}

type Projection interface {
	Fetch() (core.Iterator, error)
	Callback(eventsourcing.Event) error
}

// RegisterProjectionsAsGroup registers a group of projections
func RegisterProjectionsAsGroup(repo *eventsourcing.EventRepository, ps ...Projection) *eventsourcing.Group {
	esps := make([]*eventsourcing.Projection, len(ps))
	for i, p := range ps {
		esps[i] = repo.Projections.Projection(p.Fetch, p.Callback)
		esps[i].Pace = time.Second * 2
	}

	return repo.Projections.Group(esps...)
}

// ResetAllProjections resets all projections by dropping the projections table
func ResetAllProjections(db *sql.DB) error {
	_, err := db.Exec(`drop table if exists projections;`)
	return err
}
