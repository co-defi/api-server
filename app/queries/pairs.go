package queries

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/hallgren/eventsourcing"
	"github.com/huandu/go-sqlbuilder"
)

var _ common.Projection = (*PairsQuery)(nil)

// PairsQuery is a query that keeps track of all pairs
type PairsQuery struct {
	*common.BaseProjection
}

// NewPairsQuery creates a new PairsQuery
func NewPairsQuery(db *sql.DB, store common.Store) (*PairsQuery, error) {
	bp, err := common.NewBaseProjection(db, store, "pairs_query")
	if err != nil {
		return nil, err
	}

	pq := PairsQuery{bp}
	if err := pq.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create pairs_query table: %w", err)
	}

	return &pq, nil
}

func (pq *PairsQuery) createTable() error {
	_, err := pq.Exec(`create table if not exists pairs_query (
		id VARCHAR PRIMARY KEY,
		status TEXT,
		assets BLOB,
		participant_addresses BLOB,
		share_value INTEGER,
		investing_period INTEGER,
		wallet_security TEXT,
		profit_sharing_strategy TEXT,
		loss_protection REAL,
		wallet BLOB,
		assurances BLOB,
		deposits BLOB,
		withdraw_tx BLOB,
		lp BLOB,
		deadline TEXT,
		withdrawn_tx TEXT,
		created_at TEXT,
		updated_at TEXT
	);`)
	return err
}

// Callback implements the common.Projection.Callback
func (pq *PairsQuery) Callback(event eventsourcing.Event) error {
	switch e := event.Data().(type) {
	case *domain.PairCreated:
		if err := pq.insertPair(event, e); err != nil {
			return fmt.Errorf("failed to insert pair: %w", err)
		}
	case *domain.PairStatusChanged:
		if err := pq.updateStatus(event, e.Status); err != nil {
			return fmt.Errorf("failed to update pair status: %w", err)
		}
	}

	if err := pq.Increment(); err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}

	return nil
}

func (pq *PairsQuery) insertPair(event eventsourcing.Event, e *domain.PairCreated) error {
	ts := event.Timestamp().Format(time.RFC3339)
	_, err := pq.Exec(`insert into pairs_query (
		id,
		assets,
		participant_addresses,
		share_value,
		investing_period,
		wallet_security,
		profit_sharing_strategy,
		loss_protection,
		wallet,
		assurances,
		deposits,
		withdraw_tx,
		lp,
		deadline,
		withdrawn_tx,
		created_at,
		updated_at) values (?, ?, jsonb(?), jsonb(?), ?, ?, ?, ?, ?, jsonb(?), jsonb(?), jsonb(?), jsonb(?), ?, jsonb(?), ?, ?);`,
		event.AggregateID(),
		mustMarshalJson([]domain.Asset{e.ParticipantAsset, e.SecondaryAsset}),
		mustMarshalJson([]domain.Address{e.ParticipantAddress, domain.EmptyAddress}),
		e.ShareValue,
		e.InvestingPeriod,
		e.WalletSecurity,
		e.ProfitSharingStrategy,
		e.LossProtection,
		mustMarshalJson(nil),
		mustMarshalJson(map[domain.Asset][]domain.SignedTx{}),
		mustMarshalJson(map[domain.Asset]domain.TxHash{}),
		mustMarshalJson(nil),
		mustMarshalJson(nil),
		nil,
		nil,
		ts,
		ts,
	)
	return err
}

func mustMarshalJson(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func (pq *PairsQuery) updateStatus(event eventsourcing.Event, status domain.PairStatus) error {
	_, err := pq.Exec(`update pairs_query set status = ?, updated_at = ? where id = ?;`,
		string(status), event.Timestamp().Format(time.RFC3339), event.AggregateID())
	return err
}

// Pair represents a pair
type Pair struct {
	Id                    string                             `json:"id"`
	Status                domain.PairStatus                  `json:"status"`
	Assets                []domain.Asset                     `json:"assets"`
	ParticipantAddresses  []domain.Address                   `json:"participant_addresses"`
	ShareValue            int                                `json:"share_value"`
	InvestingPeriod       int                                `json:"investing_period"`
	WalletSecurity        domain.MultiSigWalletSecurity      `json:"wallet_security"`
	ProfitSharingStrategy domain.ProfitSharingStrategy       `json:"profit_sharing_strategy"`
	LossProtection        float64                            `json:"loss_protection"`
	Wallet                *domain.MultisigWallet             `json:"wallet"`
	Assurances            map[domain.Asset][]domain.SignedTx `json:"assurances"`
	Deposits              map[domain.Asset]domain.TxHash     `json:"deposits"`
	WithdrawTx            *domain.SignedTx                   `json:"withdraw_tx"`
	LP                    map[domain.Asset]domain.TxHash     `json:"lp"`
	Deadline              *time.Time                         `json:"deadline"`
	WithdrawnTx           *domain.TxHash                     `json:"withdrawn_tx"`
	CreatedAt             time.Time                          `json:"created_at"`
	UpdatedAt             time.Time                          `json:"updated_at"`
}

// Find finds pairs by given conditions
func (pq *PairsQuery) Find(
	ctx context.Context,
	status *domain.PairStatus,
	assets []domain.Asset,
	assetsOrder bool,
	participantAddresses []domain.Address,
	shareValue *int,
	investingPeriod *int,
	walletSecurity *domain.MultiSigWalletSecurity,
	profitSharingStrategy *domain.ProfitSharingStrategy,
	lossProtection *float64,
) ([]Pair, error) {
	b := sqlbuilder.NewSelectBuilder()
	b.SetFlavor(sqlbuilder.SQLite)
	b.Select(
		"id",
		"status",
		"json(assets)",
		"json(participant_addresses)",
		"share_value",
		"investing_period",
		"wallet_security",
		"profit_sharing_strategy",
		"loss_protection",
		"json(wallet)",
		"json(assurances)",
		"json(deposits)",
		"json(withdraw_tx)",
		"json(lp)",
		"deadline",
		"withdrawn_tx",
		"created_at",
		"updated_at",
	).From("pairs_query")
	if status != nil {
		b.Where(b.Equal("status", string(*status)))
	}
	if len(assets) > 0 {
		if len(assets) > 1 {
			if assetsOrder {
				b.Where(fmt.Sprintf("assets = jsonb(%s)", b.Var(mustMarshalJson(assets))))
			} else {
				b.Where(fmt.Sprintf("exists (select 1 from json_each(pairs_query.assets) as assets_array where assets_array.value = %s or assets_array.value = %s)",
					b.Var(assets[0]), b.Var(assets[1])))
			}
		} else {
			b.Where(fmt.Sprintf("exists (select 1 from json_each(pairs_query.assets) as assets_array where assets_array.value = %s)", b.Var(assets[0])))
		}
	}
	if len(participantAddresses) > 0 {
		b.Where(b.Exists(buildParticipantAddressesWhereClause(participantAddresses)))
	}
	if shareValue != nil {
		b.Where(b.Equal("share_value", *shareValue))
	}
	if investingPeriod != nil {
		b.Where(b.Equal("investing_period", *investingPeriod))
	}
	if walletSecurity != nil {
		b.Where(b.Equal("wallet_security", string(*walletSecurity)))
	}
	if profitSharingStrategy != nil {
		b.Where(b.Equal("profit_sharing_strategy", string(*profitSharingStrategy)))
	}
	if lossProtection != nil {
		b.Where(b.Equal("loss_protection", *lossProtection))
	}

	query, args := b.Build()
	rows, err := pq.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query pairs: %w", err)
	}
	defer rows.Close()

	pairs := []Pair{}
	for rows.Next() {
		var (
			id                    string
			status                string
			assets                []byte
			participantAddresses  []byte
			shareValue            int
			investingPeriod       int
			walletSecurity        string
			profitSharingStrategy string
			lossProtection        float64
			wallet                []byte
			assurances            []byte
			deposits              []byte
			withdrawTx            []byte
			lp                    []byte
			deadline              sql.NullString
			withdrawnTx           sql.NullString
			createdAt             string
			updatedAt             string
		)
		if err := rows.Scan(
			&id,
			&status,
			&assets,
			&participantAddresses,
			&shareValue,
			&investingPeriod,
			&walletSecurity,
			&profitSharingStrategy,
			&lossProtection,
			&wallet,
			&assurances,
			&deposits,
			&withdrawTx,
			&lp,
			&deadline,
			&withdrawnTx,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan pair: %w", err)
		}
		p := Pair{
			Id:                    id,
			Status:                domain.PairStatus(status),
			Assets:                mustUnmarshalToType[[]domain.Asset](assets),
			ParticipantAddresses:  mustUnmarshalToType[[]domain.Address](participantAddresses),
			ShareValue:            shareValue,
			InvestingPeriod:       investingPeriod,
			WalletSecurity:        domain.MultiSigWalletSecurity(walletSecurity),
			ProfitSharingStrategy: domain.ProfitSharingStrategy(profitSharingStrategy),
			LossProtection:        lossProtection,
			Wallet:                mustUnmarshalToPointer[domain.MultisigWallet](wallet),
			Assurances:            mustUnmarshalToType[map[domain.Asset][]domain.SignedTx](assurances),
			Deposits:              mustUnmarshalToType[map[domain.Asset]domain.TxHash](deposits),
			WithdrawTx:            mustUnmarshalToPointer[domain.SignedTx](withdrawTx),
			LP:                    mustUnmarshalToType[map[domain.Asset]domain.TxHash](lp),
			Deadline:              nullStringToTime(deadline),
			WithdrawnTx:           (*domain.TxHash)(nullStringToPointer(withdrawnTx)),
			CreatedAt:             mustParseTime(createdAt),
			UpdatedAt:             mustParseTime(updatedAt),
		}
		pairs = append(pairs, p)
	}

	return pairs, nil
}

func buildAssetsWhereClause(b *sqlbuilder.SelectBuilder, assets []domain.Asset) string {
	if len(assets) > 1 {
		return fmt.Sprintf("SELECT 1 FROM json_each(pairs_query.assets) as assets_array WHERE assets_array.value = %s OR assets_array.value = %s", b.Var(assets[0]), b.Var(assets[1]))
	} else {
		return fmt.Sprintf("SELECT 1 FROM json_each(pairs_query.assets) as assets_array WHERE assets_array.value = %s", b.Var(assets[0]))
	}
}

func buildParticipantAddressesWhereClause(participantAddresses []domain.Address) string {
	b := sqlbuilder.NewSelectBuilder()
	b.Select("1").From("json_each(pairs_query.participant_addresses) as participant_addresses_array")
	if len(participantAddresses) == 2 {
		b.Where(b.Or(
			b.Equal("participant_addresses_array.value", participantAddresses[0]),
			b.Equal("participant_addresses_array.value", participantAddresses[1]),
		))
	} else {
		b.Where(b.Equal("participant_addresses_array.value", participantAddresses[0]))
	}
	return b.String()
}

func mustUnmarshalToType[T any](b []byte) T {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	return v
}

func mustUnmarshalToPointer[T any](b []byte) *T {
	var v *T
	if err := json.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	return v
}

func nullStringToTime(ns sql.NullString) *time.Time {
	if ns.Valid {
		t := mustParseTime(ns.String)
		return &t
	}
	return nil
}

func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}

func nullStringToPointer(ns sql.NullString) *string {
	if ns.Valid {
		return &ns.String
	}
	return nil
}

var ErrPairNotFound = common.NewError("pair_not_found", "pair not found")

// Get gets a pair by id
func (pq *PairsQuery) Get(ctx context.Context, id string) (*Pair, error) {
	row := pq.QueryRowContext(
		ctx,
		`select
			id,
			status,
			json(assets),
			json(participant_addresses),
			share_value,
			investing_period,
			wallet_security,
			profit_sharing_strategy,
			loss_protection,
			json(wallet),
			json(assurances),
			json(deposits),
			json(withdraw_tx),
			json(lp),
			deadline,
			withdrawn_tx,
			created_at,
			updated_at
		from pairs_query where id = ?;`,
		id,
	)

	var (
		status                string
		assets                []byte
		participantAddresses  []byte
		shareValue            int
		investingPeriod       int
		walletSecurity        string
		profitSharingStrategy string
		lossProtection        float64
		wallet                []byte
		assurances            []byte
		deposits              []byte
		withdrawTx            []byte
		lp                    []byte
		deadline              sql.NullString
		withdrawnTx           sql.NullString
		createdAt             string
		updatedAt             string
	)
	if err := row.Scan(
		&id,
		&status,
		&assets,
		&participantAddresses,
		&shareValue,
		&investingPeriod,
		&walletSecurity,
		&profitSharingStrategy,
		&lossProtection,
		&wallet,
		&assurances,
		&deposits,
		&withdrawTx,
		&lp,
		&deadline,
		&withdrawnTx,
		&createdAt,
		&updatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrPairNotFound
		}

		return nil, fmt.Errorf("failed to scan pair: %w", err)
	}
	p := Pair{
		Id:                    id,
		Status:                domain.PairStatus(status),
		Assets:                mustUnmarshalToType[[]domain.Asset](assets),
		ParticipantAddresses:  mustUnmarshalToType[[]domain.Address](participantAddresses),
		ShareValue:            shareValue,
		InvestingPeriod:       investingPeriod,
		WalletSecurity:        domain.MultiSigWalletSecurity(walletSecurity),
		ProfitSharingStrategy: domain.ProfitSharingStrategy(profitSharingStrategy),
		LossProtection:        lossProtection,
		Wallet:                mustUnmarshalToPointer[domain.MultisigWallet](wallet),
		Assurances:            mustUnmarshalToType[map[domain.Asset][]domain.SignedTx](assurances),
		Deposits:              mustUnmarshalToType[map[domain.Asset]domain.TxHash](deposits),
		WithdrawTx:            mustUnmarshalToPointer[domain.SignedTx](withdrawTx),
		LP:                    mustUnmarshalToType[map[domain.Asset]domain.TxHash](lp),
		Deadline:              nullStringToTime(deadline),
		WithdrawnTx:           (*domain.TxHash)(nullStringToPointer(withdrawnTx)),
		CreatedAt:             mustParseTime(createdAt),
		UpdatedAt:             mustParseTime(updatedAt),
	}
	return &p, nil
}
