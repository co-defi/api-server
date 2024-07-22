package app

import (
	"database/sql"
	"fmt"

	"github.com/co-defi/api-server/app/commands"
	"github.com/co-defi/api-server/app/queries"
	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing"
	sqles "github.com/hallgren/eventsourcing/eventstore/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type Application struct {
	Commands Commands
	Queries  Queries

	projectionsGroup *eventsourcing.Group
	logger           zerolog.Logger
}

func NewApplication(db *sql.DB) (*Application, error) {
	// Set how identifiers are generated on newly created aggregates
	eventsourcing.SetIDFunc(func() string {
		return uuid.New().String()
	})

	repo, store, err := createEventRepository(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create event repository: %w", err)
	}

	queries, err := newQueries(db, store)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare queries: %w", err)
	}

	app := Application{
		Commands: Commands{
			CreateNewPlan:     commands.NewCreateNewPlanHandler(repo),
			CreateOrMatchPair: commands.NewCreateOrMatchPairHandler(repo, queries.Plans, queries.Pairs),
			ConfirmPairWallet: commands.NewConfirmPairWalletHandler(repo),
			SetPairAssurances: commands.NewSetPairAssurancesHandler(repo),
			AddDeposit:        commands.NewAddDepositHandler(repo),
			SignWithdrawal:    commands.NewSignWithdrawalHandler(repo),
			SubmitLP:          commands.NewSubmitLPHandler(repo),
			SubmitWithdrawal:  commands.NewSubmitWithdrawalHandler(repo),
		},
		Queries: queries,
		logger:  zerolog.Nop(),
	}

	app.registerProjections(repo)

	return &app, nil
}

func createEventRepository(db *sql.DB) (*eventsourcing.EventRepository, *sqles.SQL, error) {
	store := sqles.Open(db)

	need, err := needMigration(db)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check if migration is needed: %w", err)
	}

	if need {
		err = store.Migrate()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to migrate event store: %w", err)
		}
	}

	repo := eventsourcing.NewEventRepository(store)
	registerAggregates(repo)

	return repo, store, nil
}

func needMigration(db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRow("select count(*) from sqlite_master where type='table' and name='events';").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if migration is needed: %w", err)
	}

	return count == 0, nil
}

func registerAggregates(repo *eventsourcing.EventRepository) {
	repo.Register(&domain.Plan{})
	repo.Register(&domain.Pair{})
}

func (app *Application) registerProjections(repo *eventsourcing.EventRepository) {
	app.projectionsGroup = common.RegisterProjectionsAsGroup(
		repo,
		app.Queries.Plans,
		app.Queries.Pairs,
	)
}

func (app *Application) handleProjectionErrors() {
	for res := range app.projectionsGroup.ErrChan {
		app.logger.Error().Err(res.Error).Str("projection", res.Name).Msg("projection error")
	}
}

// WithLogger sets the logger for the application
func (app *Application) WithLogger(logger zerolog.Logger) {
	app.logger = logger
}

// StartProjections starts the projections
func (app *Application) StartProjections() {
	go app.handleProjectionErrors()
	go app.projectionsGroup.Start()
}

// StopProjections stops the projections
func (app *Application) StopProjections() {
	if app.projectionsGroup != nil {
		app.projectionsGroup.Stop()
	}
}

type Commands struct {
	CreateNewPlan     commands.CreateNewPlanHandler
	CreateOrMatchPair commands.CreateOrMatchPairHandler
	ConfirmPairWallet commands.ConfirmPairWalletHandler
	SetPairAssurances commands.SetPairAssurancesHandler
	AddDeposit        commands.AddDepositHandler
	SignWithdrawal    commands.SignWithdrawalHandler
	SubmitLP          commands.SubmitLPHandler
	SubmitWithdrawal  commands.SubmitWithdrawalHandler
}

type Queries struct {
	Plans *queries.PlansQuery
	Pairs *queries.PairsQuery
}

func newQueries(db *sql.DB, store *sqles.SQL) (Queries, error) {
	plans, err := queries.NewPlansQuery(db, store)
	if err != nil {
		return Queries{}, fmt.Errorf("failed to create plans query: %w", err)
	}

	pairs, err := queries.NewPairsQuery(db, store)
	if err != nil {
		return Queries{}, fmt.Errorf("failed to create pairs query: %w", err)
	}

	return Queries{
		Plans: plans,
		Pairs: pairs,
	}, nil
}
