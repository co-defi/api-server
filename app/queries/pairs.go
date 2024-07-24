package queries

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
		assets TEXT,
		participant_addresses TEXT,
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
	tx, err := pq.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	switch e := event.Data().(type) {
	case *domain.PairCreated:
		if err := insertPair(tx, event, e); err != nil {
			return fmt.Errorf("failed to insert pair: %w", err)
		}
	case *domain.PairStatusChanged:
		if err := updateStatus(tx, event, e.Status); err != nil {
			return fmt.Errorf("failed to update pair status: %w", err)
		}
	case *domain.PairMatched:
		if err := setPairMatched(tx, event, e); err != nil {
			return fmt.Errorf("failed to set pair matched: %w", err)
		}
	case *domain.WalletAddressConfirmed:
		if err := updateMultisigWallet(tx, event, e); err != nil {
			return fmt.Errorf("failed to update pair status: %w", err)
		}
	case *domain.AssetAssuranceSigned:
		if err := updateAssurances(tx, event, e); err != nil {
			return fmt.Errorf("failed to update assurances: %w", err)
		}
	case *domain.AssetDeposited:
		if err := updateDeposits(tx, event, e); err != nil {
			return fmt.Errorf("failed to update deposits: %w", err)
		}
	case *domain.WithdrawTxSigned:
		if err := updateWithdrawTx(tx, event, e); err != nil {
			return fmt.Errorf("failed to update withdraw tx: %w", err)
		}
	case *domain.LPDone:
		if err := updateLP(tx, event, e); err != nil {
			return fmt.Errorf("failed to update LP: %w", err)
		}
	case *domain.Withdrawn:
		if err := updateWithdrawnTx(tx, event, e.TxHash); err != nil {
			return fmt.Errorf("failed to update withdrawn tx: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

func insertPair(tx executor, event eventsourcing.Event, e *domain.PairCreated) error {
	ts := event.Timestamp().Format(time.RFC3339)
	_, err := tx.Exec(`insert into pairs_query (
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
		updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, jsonb(?), jsonb(?), jsonb(?), jsonb(?), ?, jsonb(?), ?, ?);`,
		event.AggregateID(),
		strings.Join(assetsToStrings([]domain.Asset{e.ParticipantAsset, e.SecondaryAsset}), ","),
		e.ParticipantAddress,
		e.ShareValue,
		e.InvestingPeriod,
		e.WalletSecurity,
		e.ProfitSharingStrategy,
		e.LossProtection,
		mustMarshalJson(domain.MultisigWallet{}),
		mustMarshalJson(map[domain.Asset][]domain.SignedTx{}),
		mustMarshalJson(map[domain.Asset]domain.TxHash{}),
		mustMarshalJson(nil),
		mustMarshalJson(map[domain.Asset]domain.TxHash{}),
		nil,
		nil,
		ts,
		ts,
	)
	return err
}

type executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func mustMarshalJson(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func addressesToStrings(addresses []domain.Address) []string {
	strs := make([]string, len(addresses))
	for i, a := range addresses {
		strs[i] = string(a)
	}
	return strs
}

func updateStatus(tx executor, event eventsourcing.Event, status domain.PairStatus) error {
	_, err := tx.Exec(`update pairs_query set status = ?, updated_at = ? where id = ?;`,
		status, event.Timestamp().Format(time.RFC3339), event.AggregateID())
	return err
}

func setPairMatched(tx executor, event eventsourcing.Event, e *domain.PairMatched) error {
	_, err := tx.Exec(`update pairs_query set 
	participant_addresses = format('%s,%s', participant_addresses, ?), 
	wallet = jsonb_set(jsonb_set(wallet, '$.encryption_key', ?), '$.hex_chain_code', ?),
	updated_at = ? 
	where id = ?;`,
		e.ParticipantAddress,
		e.WalletEncryptionKey,
		e.WalletHexChainCode,
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateMultisigWallet(tx executor, event eventsourcing.Event, e *domain.WalletAddressConfirmed) error {
	_, err := tx.Exec(`update pairs_query set 
		wallet = jsonb_set(jsonb_set(wallet, format('$.public_keys."%s"', ?), ?), '$.addresses', jsonb(?)),
		updated_at = ? 
		where id = ?;`,
		e.ParticipantAsset,
		e.PublicKey,
		mustMarshalJson(e.WalletAddresses),
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateAssurances(tx executor, event eventsourcing.Event, e *domain.AssetAssuranceSigned) error {
	_, err := tx.Exec(`update pairs_query set
		assurances = jsonb_set(assurances, format('$."%s"[#]', ?), jsonb(?)),
		updated_at = ?
		where id = ?;`,
		e.Asset,
		mustMarshalJson(e.Tx),
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateDeposits(tx executor, event eventsourcing.Event, e *domain.AssetDeposited) error {
	_, err := tx.Exec(`update pairs_query set
		deposits = jsonb_set(deposits, format('$."%s"', ?), ?),
		updated_at = ?
		where id = ?;`,
		e.Asset,
		e.TxHash,
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateWithdrawTx(tx executor, event eventsourcing.Event, e *domain.WithdrawTxSigned) error {
	_, err := tx.Exec(`update pairs_query set
		withdraw_tx = jsonb(?),
		updated_at = ?
		where id = ?;`,
		mustMarshalJson(e.Tx),
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateLP(tx executor, event eventsourcing.Event, e *domain.LPDone) error {
	_, err := tx.Exec(`update pairs_query set
		lp = jsonb_set(lp, format('$."%s"', ?), ?),
		deadline = ?,
		updated_at = ?
		where id = ?;`,
		e.Asset,
		e.TxHash,
		e.Deadline.Format(time.RFC3339),
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
	return err
}

func updateWithdrawnTx(tx executor, event eventsourcing.Event, txHash domain.TxHash) error {
	_, err := tx.Exec(`update pairs_query set
		withdrawn_tx = ?,
		updated_at = ?
		where id = ?;`,
		txHash,
		event.Timestamp().Format(time.RFC3339),
		event.AggregateID(),
	)
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
// TODO: Add pagination and order by
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
		"assets",
		"participant_addresses",
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
				b.Where(b.Equal("assets", strings.Join(assetsToStrings(assets), ",")))
			} else {
				b.Where(b.And(
					b.Like("assets", fmt.Sprintf("%%%s%%", assets[0])),
					b.Like("assets", fmt.Sprintf("%%%s%%", assets[1])),
				))
			}
		} else {
			b.Where(b.Like("assets", fmt.Sprintf("%%%s%%", assets[0])))
		}
	}
	if len(participantAddresses) > 0 {
		if len(participantAddresses) > 1 {
			b.Where(b.And(
				b.Like("participant_addresses", fmt.Sprintf("%%%s%%", participantAddresses[0])),
				b.Like("participant_addresses", fmt.Sprintf("%%%s%%", participantAddresses[1])),
			))
		} else {
			b.Where(b.Like("participant_addresses", fmt.Sprintf("%%%s%%", participantAddresses[0])))
		}
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
			assets                string
			participantAddresses  string
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
			Assets:                stringsToAssets(strings.Split(assets, ",")),
			ParticipantAddresses:  stringsToAddresses(strings.Split(participantAddresses, ",")),
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

func stringsToAddresses(strs []string) []domain.Address {
	addresses := make([]domain.Address, len(strs))
	for i, s := range strs {
		addresses[i] = domain.Address(s)
	}
	return addresses
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
			assets,
			participant_addresses,
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
		assets                string
		participantAddresses  string
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
		Assets:                stringsToAssets(strings.Split(assets, ",")),
		ParticipantAddresses:  stringsToAddresses(strings.Split(participantAddresses, ",")),
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
