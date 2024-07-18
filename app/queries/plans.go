package queries

import (
	"context"
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
		loss_protection REAL,
		investing_period INTEGER
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
	_, err := pq.Exec(`insert into plans_query (id, assets, security, strategy, quantum, loss_protection, investing_period) values (?, ?, ?, ?, ?, ?, ?);`,
		id, strings.Join(assetsToStrings(e.Assets), ","), e.Security, e.Strategy, e.Quantum, e.LossProtection, e.InvestingPeriod)
	return err
}

func assetsToStrings(assets []domain.Asset) []string {
	strs := make([]string, len(assets))
	for i, a := range assets {
		strs[i] = string(a)
	}
	return strs
}

type Plan struct {
	Id              string                        `json:"id"`
	Assets          []domain.Asset                `json:"assets"`
	Security        domain.MultiSigWalletSecurity `json:"security"`
	Strategy        domain.ProfitSharingStrategy  `json:"strategy"`
	Quantum         int                           `json:"quantum"`
	LossProtection  float64                       `json:"loss_protection"`
	InvestingPeriod int                           `json:"investing_period"`
}

// All returns all plans
func (pq *PlansQuery) All(ctx context.Context) ([]Plan, error) {
	rows, err := pq.QueryContext(ctx, `select * from plans_query;`)
	if err != nil {
		return nil, fmt.Errorf("failed to query plans: %w", err)
	}
	defer rows.Close()

	plans := []Plan{}
	for rows.Next() {
		var (
			id              string
			assets          string
			security        string
			strategy        string
			quantum         int
			LossProtection  float64
			investingPeriod int
		)
		if err := rows.Scan(&id, &assets, &security, &strategy, &quantum, &LossProtection, &investingPeriod); err != nil {
			return nil, fmt.Errorf("failed to scan plan: %w", err)
		}
		plans = append(plans, Plan{
			Id:              id,
			Assets:          stringsToAssets(strings.Split(assets, ",")),
			Security:        domain.MultiSigWalletSecurity(security),
			Strategy:        domain.ProfitSharingStrategy(strategy),
			Quantum:         quantum,
			LossProtection:  LossProtection,
			InvestingPeriod: investingPeriod,
		})
	}

	return plans, nil
}

func stringsToAssets(strs []string) []domain.Asset {
	assets := make([]domain.Asset, len(strs))
	for i, s := range strs {
		assets[i] = domain.Asset(s)
	}
	return assets
}

var ErrPlanNotFound = common.NewError("plan_not_found", "plan not found")

// Get returns a plan by id
func (pq *PlansQuery) Get(ctx context.Context, id string) (*Plan, error) {
	row := pq.QueryRowContext(ctx, `select * from plans_query where id = ?;`, id)

	var (
		assets          string
		security        string
		strategy        string
		quantum         int
		lossProtection  float64
		investingPeriod int
	)
	if err := row.Scan(&id, &assets, &security, &strategy, &quantum, &lossProtection, &investingPeriod); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrPlanNotFound
		}
		return nil, fmt.Errorf("failed to scan plan: %w", err)
	}

	return &Plan{
		Id:              id,
		Assets:          stringsToAssets(strings.Split(assets, ",")),
		Security:        domain.MultiSigWalletSecurity(security),
		Strategy:        domain.ProfitSharingStrategy(strategy),
		Quantum:         quantum,
		LossProtection:  lossProtection,
		InvestingPeriod: investingPeriod,
	}, nil
}
