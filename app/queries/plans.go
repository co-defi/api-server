package queries

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/hallgren/eventsourcing"
)

var _ common.Projection = (*PlansQuery)(nil)

// PlansQuery is a query that keeps track of all plans
type PlansQuery struct {
	*common.BaseProjection
}

// NewPlansQuery creates a new PlansQuery
func NewPlansQuery(db *sql.DB, store common.Store) (*PlansQuery, error) {
	bp, err := common.NewBaseProjection(db, store, "plans_query")
	if err != nil {
		return nil, err
	}

	pq := PlansQuery{bp}
	if err := pq.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create plans_query table: %w", err)
	}

	return &pq, nil
}

func (pq *PlansQuery) createTable() error {
	_, err := pq.Exec(`create table if not exists plans_query (
		id VARCHAR PRIMARY KEY,
		assets TEXT,
		security TEXT,
		strategy TEXT,
		quantum INTEGER,
		loss_limit REAL,
		time_frame INTEGER
	);`)
	return err
}

// Callback implements the common.Projection.Callback
func (pq *PlansQuery) Callback(event eventsourcing.Event) error {
	switch e := event.Data().(type) {
	case *domain.PlanCreated:
		if err := pq.insertPlan(event.AggregateID(), e); err != nil {
			return fmt.Errorf("failed to insert plan: %w", err)
		}
	}

	if err := pq.Increment(); err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}

	return nil
}

func (pq *PlansQuery) insertPlan(id string, e *domain.PlanCreated) error {
	_, err := pq.Exec(`insert into plans_query (id, assets, security, strategy, quantum, loss_limit, time_frame) values (?, ?, ?, ?, ?, ?, ?);`,
		id, strings.Join(e.Assets, ","), e.Security, e.Strategy, e.Quantum, e.LossLimit, e.TimeFrame)
	return err
}

type Plan struct {
	Id        string          `json:"id,omitempty"`
	Assets    []string        `json:"assets,omitempty"`
	Security  domain.Security `json:"security,omitempty"`
	Strategy  domain.Strategy `json:"strategy,omitempty"`
	Quantum   int             `json:"quantum,omitempty"`
	LossLimit float64         `json:"loss_limit,omitempty"`
	TimeFrame int             `json:"time_frame,omitempty"`
}

// All returns all plans
func (pq *PlansQuery) All() ([]Plan, error) {
	rows, err := pq.Query(`select * from plans_query;`)
	if err != nil {
		return nil, fmt.Errorf("failed to query plans: %w", err)
	}
	defer rows.Close()

	plans := []Plan{}
	for rows.Next() {
		var (
			id        string
			assets    string
			security  string
			strategy  string
			quantum   int
			lossLimit float64
			timeFrame int
		)
		if err := rows.Scan(&id, &assets, &security, &strategy, &quantum, &lossLimit, &timeFrame); err != nil {
			return nil, fmt.Errorf("failed to scan plan: %w", err)
		}
		plans = append(plans, Plan{
			Id:        id,
			Assets:    strings.Split(assets, ","),
			Security:  domain.Security(security),
			Strategy:  domain.Strategy(strategy),
			Quantum:   quantum,
			LossLimit: lossLimit,
			TimeFrame: timeFrame,
		})
	}

	return plans, nil
}
