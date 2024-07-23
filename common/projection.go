package common

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/core"
	"github.com/rs/zerolog"
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

// Fetch fetches events from the store
func (bp *BaseProjection) Fetch() (core.Iterator, error) {
	lastStart, err := bp.getLastHandledEventSeq()
	if err != nil {
		return nil, fmt.Errorf("failed to get last processed event seq: %w", err)
	}

	it, err := bp.store.All(core.Version(lastStart+1), projectionCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create events iterator: %w", err)
	}

	return newCacheIterator(it)
}

type cacheIterator struct {
	events []core.Event
	pos    int
}

func newCacheIterator(it core.Iterator) (*cacheIterator, error) {
	var events []core.Event
	for it.Next() {
		e, err := it.Value()
		if err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	return &cacheIterator{events: events, pos: -1}, nil
}

func (ci *cacheIterator) Next() bool {
	ci.pos++
	return ci.pos < len(ci.events)
}

func (ci *cacheIterator) Value() (core.Event, error) {
	return ci.events[ci.pos], nil
}

func (ci *cacheIterator) Close() {}

// Begin starts a new transaction for the projection
func (bp *BaseProjection) Begin() (*sql.Tx, error) {
	tx, err := bp.DB.Begin()
	if err != nil {
		return nil, err
	}

	err = incrementProjection(tx, bp.name)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func incrementProjection(tx *sql.Tx, name string) error {
	_, err := tx.Exec(`update projections set last_handled_event_seq = last_handled_event_seq + 1 where id = ?;`, name)
	return err
}

// Store is an interface that limits the methods that can be called on a store to All method only
type Store interface {
	All(start core.Version, count uint64) (core.Iterator, error)
}

type FailSafeProjection struct {
	base             Projection
	logger           zerolog.Logger
	fetchFailures    int
	callbackFailures int
}

func NewFailSafeProjection(base Projection, logger zerolog.Logger) *FailSafeProjection {
	return &FailSafeProjection{
		base:   base,
		logger: logger,
	}
}

// Fetch implements the Fetch method of the Projection interface
func (fsp *FailSafeProjection) Fetch() (core.Iterator, error) {
	it, err := fsp.base.Fetch()
	if err != nil {
		fsp.fetchFailures++
		fsp.logger.Error().Int("fetch_failures", fsp.fetchFailures).Err(err).Msg("failed to fetch events")

		return &nopIterator{}, nil
	}

	fsp.fetchFailures = 0

	return it, err
}

type nopIterator struct{}

func (*nopIterator) Next() bool {
	return false
}

func (*nopIterator) Value() (core.Event, error) {
	return core.Event{}, nil
}

func (*nopIterator) Close() {}

// Callback implements the Callback method of the Projection interface
func (fsp *FailSafeProjection) Callback(event eventsourcing.Event) error {
	err := fsp.base.Callback(event)
	if err != nil {
		fsp.callbackFailures++
		fsp.logger.Error().Int("callback_failures", fsp.callbackFailures).Err(err).Msg("failed to handle event")

		return nil
	}

	fsp.callbackFailures = 0

	return nil
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
