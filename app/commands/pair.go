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
	ParticipantAsset      domain.Asset                  `json:"participant_asset,omitempty"`
	ParticipantAddress    domain.Address                `json:"participant_address,omitempty"`
	SecondaryAsset        domain.Asset                  `json:"secondary_asset,omitempty"`
	ShareValue            int                           `json:"share_value,omitempty"`
	InvestingPeriod       int                           `json:"investing_period,omitempty"`
	WalletSecurity        domain.MultiSigWalletSecurity `json:"wallet_security,omitempty"`
	ProfitSharingStrategy domain.ProfitSharingStrategy  `json:"profit_sharing_strategy,omitempty"`
	LossProtection        float64                       `json:"loss_protection,omitempty"`
}

// CreateOrMatchPairHandler is a command handler for CreateOrMatchPair
type CreateOrMatchPairHandler common.CommandHandler[CreateOrMatchPair]

type createOrMatchPairHandler struct {
	repo       *eventsourcing.EventRepository
	pairsQuery *queries.PairsQuery
}

// NewCreateOrMatchPairHandler creates a new CreateOrMatchPairHandler
func NewCreateOrMatchPairHandler(repo *eventsourcing.EventRepository, pq *queries.PairsQuery) *createOrMatchPairHandler {
	return &createOrMatchPairHandler{repo: repo, pairsQuery: pq}
}

// Handle implements the command handler interface
func (h *createOrMatchPairHandler) Handle(ctx context.Context, cmd CreateOrMatchPair) (string, error) {
	if err := common.Validate(cmd); err != nil {
		return "", err
	}

	var (
		status = domain.PairStatusWaiting
		assets = []domain.Asset{cmd.ParticipantAsset, cmd.SecondaryAsset}
	)
	pairs, err := h.pairsQuery.Find(
		ctx,
		&status,
		assets,
		nil,
		&cmd.ShareValue,
		&cmd.InvestingPeriod,
		&cmd.WalletSecurity,
		&cmd.ProfitSharingStrategy,
		&cmd.LossProtection,
	)
	if err != nil {
		return "", fmt.Errorf("failed to find pairs: %w", err)
	}

	if len(pairs) > 0 {
		p := domain.Pair{}
		p.SetID(uuid.New().String())
		p.TrackChange(&p, &domain.PairCreated{
			ParticipantAsset:      cmd.ParticipantAsset,
			ParticipantAddress:    cmd.ParticipantAddress,
			SecondaryAsset:        cmd.SecondaryAsset,
			ShareValue:            cmd.ShareValue,
			InvestingPeriod:       cmd.InvestingPeriod,
			WalletSecurity:        cmd.WalletSecurity,
			ProfitSharingStrategy: cmd.ProfitSharingStrategy,
			LossProtection:        cmd.LossProtection,
		})
		p.TrackChange(&p, &domain.PairStatusChanged{Status: domain.PairStatusWaiting})
		if err := h.repo.Save(&p); err != nil {
			return "", fmt.Errorf("failed to save pair: %w", err)
		}
		return p.ID(), nil
	}

	return "", fmt.Errorf("failed to find pair")
}
