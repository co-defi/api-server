package commands

import (
	"context"
	"fmt"

	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/google/uuid"
	"github.com/hallgren/eventsourcing"
)

// CreateNewPlan is a command to create a new plan
type CreateNewPlan struct {
	Id        uuid.UUID       `json:"id,omitempty"`
	Assets    []string        `json:"assets,omitempty" validate:"required,len=2"`
	Security  domain.Security `json:"security,omitempty" validate:"required,oneof=2-2"`
	Strategy  domain.Strategy `json:"strategy,omitempty" validate:"required,oneof=equal_share"`
	Quantum   int             `json:"quantum,omitempty" validate:"required,min=1"`
	LossLimit float64         `json:"loss_limit,omitempty" validate:"required,min=0.1,max=0.5"`
	TimeFrame int             `json:"time_frame,omitempty" validate:"required,min=1"`
}

// CreateNewPlanHandler is a command handler for CreateNewPlan
type CreateNewPlanHandler = common.CommandHandler[CreateNewPlan]

type createNewPlanHandler struct {
	repo *eventsourcing.EventRepository
}

// NewCreateNewPlanHandler creates a new CreateNewPlanHandler
func NewCreateNewPlanHandler(repo *eventsourcing.EventRepository) *createNewPlanHandler {
	return &createNewPlanHandler{repo: repo}
}

// Handle implements the command handler interface
func (h *createNewPlanHandler) Handle(ctx context.Context, cmd CreateNewPlan) error {
	if err := common.Validate(cmd); err != nil {
		return err
	}

	p := domain.Plan{}
	p.SetID(cmd.Id.String())
	p.TrackChange(&p, &domain.PlanCreated{
		Assets:    cmd.Assets,
		Security:  cmd.Security,
		Strategy:  cmd.Strategy,
		Quantum:   cmd.Quantum,
		LossLimit: cmd.LossLimit,
		TimeFrame: cmd.TimeFrame,
	})
	if err := h.repo.Save(&p); err != nil {
		return fmt.Errorf("failed to save plan: %w", err)
	}

	return nil
}
