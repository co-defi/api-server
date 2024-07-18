package commands

import (
	"context"
	"fmt"

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

var ErrInvalidAssetForPlan = common.NewError("invalid_asset_for_plan", "participant asset is not valid for the plan")

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
		return "", ErrInvalidAssetForPlan
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
