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
		created_at TEXT,
		updated_at TEXT,
	);`)
	return err
}

// Callback implements the common.Projection.Callback
func (pq *PairsQuery) Callback(event eventsourcing.Event) error {
	switch e := event.Data().(type) {
	case *domain.PairCreated:
		if err := pq.insertPair(event.AggregateID(), e); err != nil {
			return fmt.Errorf("failed to insert pair: %w", err)
		}
	case *domain.PairStatusChanged:
		if err := pq.updateStatus(event.AggregateID(), e.Status); err != nil {
			return fmt.Errorf("failed to update pair status: %w", err)
		}
	}

	if err := pq.Increment(); err != nil {
		return fmt.Errorf("failed to increment: %w", err)
	}

	return nil
}

func (pq *PairsQuery) insertPair(id string, e *domain.PairCreated) error {
	now := time.Now().Format(time.RFC3339)
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
		id,
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
		"",
		mustMarshalJson(nil),
		now,
		now,
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

func (pq *PairsQuery) updateStatus(id string, status domain.PairStatus) error {
	_, err := pq.Exec(`update pairs_query set status = ? where id = ?;`, status, id)
	return err
}

// Pair represents a pair
type Pair struct {
	Id                    string                             `json:"id,omitempty"`
	Assets                []domain.Asset                     `json:"assets,omitempty"`
	ParticipantAddresses  []domain.Address                   `json:"participant_addresses,omitempty"`
	ShareValue            int                                `json:"share_value,omitempty"`
	InvestingPeriod       int                                `json:"investing_period,omitempty"`
	WalletSecurity        domain.MultiSigWalletSecurity      `json:"wallet_security,omitempty"`
	ProfitSharingStrategy domain.ProfitSharingStrategy       `json:"profit_sharing_strategy,omitempty"`
	LossProtection        float64                            `json:"loss_protection,omitempty"`
	Wallet                *domain.MultisigWallet             `json:"wallet,omitempty"`
	Assurances            map[domain.Asset][]domain.SignedTx `json:"assurances,omitempty"`
	Deposits              map[domain.Asset]domain.TxHash     `json:"deposits,omitempty"`
	WithdrawTx            *domain.SignedTx                   `json:"withdraw_tx,omitempty"`
	LP                    map[domain.Asset]domain.TxHash     `json:"lp,omitempty"`
	Deadline              time.Time                          `json:"deadline,omitempty"`
	WithdrawnTx           *domain.TxHash                     `json:"withdrawn_tx,omitempty"`
	CreatedAt             time.Time                          `json:"created_at,omitempty"`
	UpdatedAt             time.Time                          `json:"updated_at,omitempty"`
}

// Find finds pairs by given conditions
func (pq *PairsQuery) Find(
	ctx context.Context,
	status *domain.PairStatus,
	assets []domain.Asset,
	participantAddresses []domain.Address,
	shareValue *int,
	investingPeriod *int,
	walletSecurity *domain.MultiSigWalletSecurity,
	profitSharingStrategy *domain.ProfitSharingStrategy,
	lossProtection *float64,
) ([]Pair, error) {
	b := sqlbuilder.NewSelectBuilder()
	b.Select("*").From("pairs_query")
	if status != nil {
		b.Where(b.Equal("status", string(*status)))
	}
	if len(assets) > 0 {
		b.Where(b.Exists(buildAssetsWhereClause(assets)))
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
			deadline              string
			withdrawnTx           []byte
			createdAt             string
			updatedAt             string
		)
		if err := rows.Scan(
			&id,
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
			Deadline:              mustParseTime(deadline),
			WithdrawnTx:           mustUnmarshalToPointer[domain.TxHash](withdrawnTx),
			CreatedAt:             mustParseTime(createdAt),
			UpdatedAt:             mustParseTime(updatedAt),
		}
		pairs = append(pairs, p)
	}

	return pairs, nil
}

func buildAssetsWhereClause(assets []domain.Asset) string {
	b := sqlbuilder.NewSelectBuilder()
	b.Select("1").From("json_each(pairs_query.assets) as assets_array")
	if len(assets) == 2 {
		b.Where(b.Or(
			b.Equal("assets_array.value", assets[0]),
			b.Equal("assets_array.value", assets[1]),
		))
	} else {
		b.Where(b.Equal("assets_array.value", assets[0]))
	}
	return b.String()
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

func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}
