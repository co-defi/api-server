package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/co-defi/api-server/app/queries"
	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing"
)

// CreateOrMatchPair is a command to create a new pair or match an existing pair.
type CreateOrMatchPair struct {
	PlanId             string         `json:"plan_id" validate:"required,uuid4"`
	ParticipantAsset   domain.Asset   `json:"participant_asset" validate:"required"`
	ParticipantAddress domain.Address `json:"participant_address" validate:"required"`
}

// CreateOrMatchPairHandler is a command handler for CreateOrMatchPair
type CreateOrMatchPairHandler common.CommandHandler[CreateOrMatchPair]

type createOrMatchPairHandler struct {
	repo       *eventsourcing.EventRepository
	plansQuery *queries.PlansQuery
	pairsQuery *queries.PairsQuery
}

// NewCreateOrMatchPairHandler creates a new CreateOrMatchPairHandler
func NewCreateOrMatchPairHandler(repo *eventsourcing.EventRepository, plansQuery *queries.PlansQuery, pairsQueries *queries.PairsQuery) *createOrMatchPairHandler {
	return &createOrMatchPairHandler{
		repo:       repo,
		pairsQuery: pairsQueries,
		plansQuery: plansQuery,
	}
}

var ErrInvalidAssetForPair = common.NewError("invalid_asset_for_pair", "participant asset is not valid for the pair")

// Handle implements the command handler interface
func (h *createOrMatchPairHandler) Handle(ctx context.Context, cmd CreateOrMatchPair) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	plan, err := h.plansQuery.Get(ctx, cmd.PlanId)
	if err != nil {
		return "", fmt.Errorf("failed to get plan: %w", err)
	}

	if !containsAsset(plan.Assets, cmd.ParticipantAsset) {
		return "", ErrInvalidAssetForPair
	}
	secondaryAsset := getSecondaryAsset(cmd.ParticipantAsset, plan.Assets)

	// Find a pair with the same status, secondary asset as the participant asset and primary asset as the secondary asset
	// i.e. the counterpart of the participant asset
	var status = domain.PairStatusWaiting
	pairs, err := h.pairsQuery.Find(
		ctx,
		&status,
		[]domain.Asset{secondaryAsset, cmd.ParticipantAsset},
		true,
		nil,
		&plan.Quantum,
		&plan.InvestingPeriod,
		&plan.Security,
		&plan.Strategy,
		&plan.LossProtection,
	)
	if err != nil {
		return "", fmt.Errorf("failed to find pairs: %w", err)
	}

	// If there's no suitable pair, create a new pair and wait for the counterpart
	p := domain.Pair{}
	if len(pairs) < 1 {
		p.SetID(uuid.New().String())
		p.TrackChange(&p, &domain.PairCreated{
			ParticipantAsset:      cmd.ParticipantAsset,
			ParticipantAddress:    cmd.ParticipantAddress,
			SecondaryAsset:        secondaryAsset,
			ShareValue:            plan.Quantum,
			InvestingPeriod:       plan.InvestingPeriod,
			WalletSecurity:        plan.Security,
			ProfitSharingStrategy: plan.Strategy,
			LossProtection:        plan.LossProtection,
		})
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusWaiting})
	} else {
		// If there's a suitable pair, match the pair
		err := h.repo.GetWithContext(ctx, pairs[0].Id, &p)
		if err != nil {
			return "", fmt.Errorf("failed to get pair: %w", err)
		}
		p.TrackChange(&p, &domain.PairMatched{ParticipantAddress: cmd.ParticipantAddress})
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusWalletConformation})
	}
	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}

func containsAsset(assets []domain.Asset, asset domain.Asset) bool {
	for _, a := range assets {
		if a == asset {
			return true
		}
	}
	return false
}

func getSecondaryAsset(primaryAsset domain.Asset, assets []domain.Asset) domain.Asset {
	for _, asset := range assets {
		if asset != primaryAsset {
			return asset
		}
	}
	return ""
}

// ConfirmPairWallet is a command to confirm the shared wallet addresses
type ConfirmPairWallet struct {
	PairId               string                          `json:"pair_id" validate:"required,uuid4"`
	ParticipantAsset     domain.Asset                    `json:"participant_asset" validate:"required"`
	ParticipantPublicKey string                          `json:"participant_public_key" validate:"required"`
	WalletAddresses      map[domain.Asset]domain.Address `json:"wallet_addresses" validate:"required,len=2"`
}

// ConfirmPairWalletHandler is a command handler for ConfirmPairWallet
type ConfirmPairWalletHandler common.CommandHandler[ConfirmPairWallet]

type confirmPairWalletHandler struct {
	repo       *eventsourcing.EventRepository
	pairsQuery *queries.PairsQuery
}

// NewConfirmPairWalletHandler creates a new ConfirmPairWalletHandler
func NewConfirmPairWalletHandler(repo *eventsourcing.EventRepository) *confirmPairWalletHandler {
	return &confirmPairWalletHandler{repo: repo}
}

var (
	ErrPairNotFound           = common.NewError("pair_not_found", "pair not found")
	ErrInvalidPairStatus      = common.NewError("invalid_pair_status", "pair status is not valid for this operation")
	ErrInvalidWalletAddresses = common.NewError("invalid_wallet_addresses", "wallet addresses are not the same for both participants")
)

// Handle implements the command handler interface
func (h *confirmPairWalletHandler) Handle(ctx context.Context, cmd ConfirmPairWallet) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	p := domain.Pair{}
	if err := h.repo.GetWithContext(ctx, cmd.PairId, &p); err != nil {
		if err == eventsourcing.ErrAggregateNotFound {
			return "", ErrPairNotFound
		}
		return "", fmt.Errorf("failed to get pair: %w", err)
	}

	if p.Status != domain.PairStatusWalletConformation {
		return "", ErrInvalidPairStatus
	}

	if !p.HasAsset(cmd.ParticipantAsset) {
		return "", ErrInvalidAssetForPair
	}
	for asset := range cmd.WalletAddresses {
		if !p.HasAsset(asset) {
			return "", ErrInvalidAssetForPair
		}
	}

	// TODO: Better participant identification and authentication
	if p.Wallet != nil && !p.Wallet.AreAddressesEqual(cmd.WalletAddresses) {
		return "", ErrInvalidWalletAddresses
	}

	p.TrackChange(&p, &domain.WalletAddressConfirmed{
		ParticipantAsset: cmd.ParticipantAsset,
		PublicKey:        cmd.ParticipantPublicKey,
		WalletAddresses:  cmd.WalletAddresses,
	})
	if len(p.Wallet.PublicKeys) == 2 {
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusAssurance})
	}

	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}

// SetPairAssurances is a command to set assurances for a pair
type SetPairAssurances struct {
	PairId           string            `json:"pair_id" validate:"required,uuid4"`
	ParticipantAsset domain.Asset      `json:"participant_asset" validate:"required"`
	Assurances       []domain.SignedTx `json:"assurances" validate:"required"`
}

// SetPairAssurancesHandler is a command handler for SetPairAssurances
type SetPairAssurancesHandler common.CommandHandler[SetPairAssurances]

type setPairAssurancesHandler struct {
	repo *eventsourcing.EventRepository
}

// NewSetPairAssurancesHandler creates a new SetPairAssurancesHandler
func NewSetPairAssurancesHandler(repo *eventsourcing.EventRepository) *setPairAssurancesHandler {
	return &setPairAssurancesHandler{repo: repo}
}

var ErrAlreadySetAssurances = common.NewError("already_set_assurances", "assurances are already set")

// Handle implements the command handler interface
func (h *setPairAssurancesHandler) Handle(ctx context.Context, cmd SetPairAssurances) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	if err := validateAssurances(cmd.ParticipantAsset, cmd.Assurances); err != nil {
		return "", err
	}

	p := domain.Pair{}
	if err := h.repo.GetWithContext(ctx, cmd.PairId, &p); err != nil {
		if err == eventsourcing.ErrAggregateNotFound {
			return "", ErrPairNotFound
		}
		return "", fmt.Errorf("failed to get pair: %w", err)
	}

	if p.Status != domain.PairStatusAssurance {
		return "", ErrInvalidPairStatus
	}

	if !p.HasAsset(cmd.ParticipantAsset) {
		return "", ErrInvalidAssetForPair
	}

	if p.HasAssurancesForAsset(cmd.ParticipantAsset) {
		return "", ErrAlreadySetAssurances
	}

	for _, assurance := range cmd.Assurances {
		p.TrackChange(&p, &domain.AssetAssuranceSigned{
			Asset: cmd.ParticipantAsset,
			Tx:    assurance,
		})
	}

	if len(p.Assurances) == 2 {
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusDeposit})
	}

	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}

var ErrInvalidAssurances = common.NewError("invalid_assurances", "assurances are not valid")

func validateAssurances(asset domain.Asset, assurances []domain.SignedTx) error {
	if !hasAssuranceWithNonce(assurances, 0) {
		return ErrInvalidAssurances.IncludeMeta(map[string]interface{}{"missing_assurance": "missing assurance with nonce 0"})
	}
	if !hasAssuranceWithNonce(assurances, 2) {
		return ErrInvalidAssurances.IncludeMeta(map[string]interface{}{"missing_assurance": "missing assurance with nonce 2"})
	}

	// THOR.RUNE requires assurance with nonce 4 because of the withdraw transaction
	if asset == "THOR.RUNE" {
		if !hasAssuranceWithNonce(assurances, 4) {
			return ErrInvalidAssurances.IncludeMeta(map[string]interface{}{"missing_assurance": "missing assurance with nonce 4"})
		}
	}

	return nil
}

func hasAssuranceWithNonce(assurances []domain.SignedTx, nonce int) bool {
	for _, assurance := range assurances {
		if assurance.Nonce == nonce {
			return true
		}
	}
	return false
}

// AddDeposit is a command to add a deposit to a pair
type AddDeposit struct {
	PairId string        `json:"pair_id" validate:"required,uuid4"`
	Asset  domain.Asset  `json:"asset" validate:"required"`
	TxHash domain.TxHash `json:"tx_hash" validate:"required"`
}

// AddDepositHandler is a command handler for AddDeposit
type AddDepositHandler = common.CommandHandler[AddDeposit]

type addDepositHandler struct {
	repo *eventsourcing.EventRepository
}

// NewAddDepositHandler creates a new AddDepositHandler
func NewAddDepositHandler(repo *eventsourcing.EventRepository) *addDepositHandler {
	return &addDepositHandler{repo: repo}
}

var ErrAlreadyHasDeposit = common.NewError("already_has_deposit", "pair already has a deposit for this asset")

// Handle implements the command handler interface
func (h *addDepositHandler) Handle(ctx context.Context, cmd AddDeposit) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	p := domain.Pair{}
	if err := h.repo.GetWithContext(ctx, cmd.PairId, &p); err != nil {
		if err == eventsourcing.ErrAggregateNotFound {
			return "", ErrPairNotFound
		}
		return "", fmt.Errorf("failed to get pair: %w", err)
	}

	if p.Status != domain.PairStatusDeposit {
		return "", ErrInvalidPairStatus
	}

	if !p.HasAsset(cmd.Asset) {
		return "", ErrInvalidAssetForPair
	}

	// TODO: Check the tx hash in the blockchain

	if p.HasDepositForAsset(cmd.Asset) {
		return "", ErrAlreadyHasDeposit
	}

	p.TrackChange(&p, &domain.AssetDeposited{
		Asset:  cmd.Asset,
		TxHash: cmd.TxHash,
	})

	if len(p.Deposits) == 2 {
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusPreSignWithdrawal})
	}

	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}

// SignWithdrawal is a command to sign a withdrawal transaction
type SignWithdrawal struct {
	PairId string          `json:"pair_id" validate:"required,uuid4"`
	Tx     domain.SignedTx `json:"tx" validate:"required"`
}

// SignWithdrawalHandler is a command handler for SignWithdrawal
type SignWithdrawalHandler common.CommandHandler[SignWithdrawal]

type signWithdrawalHandler struct {
	repo *eventsourcing.EventRepository
}

// NewSignWithdrawalHandler creates a new SignWithdrawalHandler
func NewSignWithdrawalHandler(repo *eventsourcing.EventRepository) *signWithdrawalHandler {
	return &signWithdrawalHandler{repo: repo}
}

func (h *signWithdrawalHandler) Handle(ctx context.Context, cmd SignWithdrawal) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	p := domain.Pair{}
	if err := h.repo.GetWithContext(ctx, cmd.PairId, &p); err != nil {
		if err == eventsourcing.ErrAggregateNotFound {
			return "", ErrPairNotFound
		}
		return "", fmt.Errorf("failed to get pair: %w", err)
	}

	if p.Status != domain.PairStatusPreSignWithdrawal {
		return "", ErrInvalidPairStatus
	}

	p.TrackChange(&p, &domain.WithdrawTxSigned{Tx: cmd.Tx})
	p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusLP})

	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}

// LPPair is a command to update the pair with LP transactions of both assets
type LPPair struct {
	PairId string        `json:"pair_id" validate:"required,uuid4"`
	Asset  domain.Asset  `json:"asset" validate:"required"`
	TxHash domain.TxHash `json:"tx_hash" validate:"required"`
}

// LPPairHandler is a command handler for LPPair
type LPPairHandler common.CommandHandler[LPPair]

type lpPairHandler struct {
	repo *eventsourcing.EventRepository
}

// NewLPPairHandler creates a new LPPairHandler
func NewLPPairHandler(repo *eventsourcing.EventRepository) *lpPairHandler {
	return &lpPairHandler{repo: repo}
}

const week = 7 * 24 * time.Hour

var ErrAlreadyHasLP = common.NewError("already_has_lp", "pair already has LP transactions for this asset")

// Handle implements the command handler interface
func (h *lpPairHandler) Handle(ctx context.Context, cmd LPPair) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	p := domain.Pair{}
	if err := h.repo.GetWithContext(ctx, cmd.PairId, &p); err != nil {
		if err == eventsourcing.ErrAggregateNotFound {
			return "", ErrPairNotFound
		}
		return "", fmt.Errorf("failed to get pair: %w", err)
	}

	if p.Status != domain.PairStatusLP {
		return "", ErrInvalidPairStatus
	}

	if !p.HasAsset(cmd.Asset) {
		return "", ErrInvalidAssetForPair
	}

	if p.HasLPForAsset(cmd.Asset) {
		return "", ErrAlreadyHasLP
	}

	p.TrackChange(&p, &domain.LPDone{
		Asset:    cmd.Asset,
		TxHash:   cmd.TxHash,
		Deadline: time.Now().Add(time.Duration(p.InvestingPeriod) * week),
	})

	if err := h.repo.Save(&p); err != nil {
		return "", fmt.Errorf("failed to save pair: %w", err)
	}

	return p.ID(), nil
}
