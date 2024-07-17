package domain

import (
	"github.com/hallgren/eventsourcing"
)

// Plan is the aggregate root for a plan that bases around a pair of crypto assets
// for liquidity providing, a security method for shared wallet authority between the parties (2 of 2) or (2 of 3 including mediator),
// a strategy for profit splitting, quantum of each asset's share in $, agreed loss limit and a time frame (in weeks) for the plan.
type Plan struct {
	eventsourcing.AggregateRoot
	Assets          []string               `json:"assets,omitempty"`
	Security        MultiSigWalletSecurity `json:"security,omitempty"`
	Strategy        ProfitSharingStrategy  `json:"strategy,omitempty"`
	Quantum         int                    `json:"quantum,omitempty"`
	LossProtection  float64                `json:"loss_protection,omitempty"`
	InvestingPeriod int                    `json:"investing_period,omitempty"`
}

// Register implements aggregate.Register
func (p *Plan) Register(r eventsourcing.RegisterFunc) {
	r(&PlanCreated{})
}

// Transition implements aggregate.Transition
func (p *Plan) Transition(event eventsourcing.Event) {
	switch e := event.Data().(type) {
	case *PlanCreated:
		p.Assets = e.Assets
		p.Security = e.Security
		p.Strategy = e.Strategy
		p.Quantum = e.Quantum
		p.LossProtection = e.LossProtection
		p.InvestingPeriod = e.InvestingPeriod
	}
}

// MultiSigWalletSecurity is the type of security method used for threshold signature wallet
// In case of 2-2, both parties need to agree on signing the withdrawal transaction and
// in case of 2-3, a third-party signer is added as mediator.
type MultiSigWalletSecurity string

const (
	MultiSigWalletSecurity2Of2 MultiSigWalletSecurity = "2-2"
	// Security2Of3 Security = "2-3"
)

// ProfitSharingStrategy is the type of profit splitting strategy used for the plan
// PriceDependent is a strategy which splits the withdrawn assets based on how much assets price has changed at the withdrawal time
// compared to the time of deposit i.e. The participant who's assets dropped more gains less of the total share in $ value.
type ProfitSharingStrategy string

const (
	ProfitSharingStrategyEqualShare ProfitSharingStrategy = "equal_share"
	// StrategyPriceDependent Strategy = "price_dependent"
)

// PlanCreated is the event for creating a new plan for the first time.
type PlanCreated struct {
	Assets          []string               `json:"assets,omitempty"`
	Security        MultiSigWalletSecurity `json:"security,omitempty"`
	Strategy        ProfitSharingStrategy  `json:"strategy,omitempty"`
	Quantum         int                    `json:"quantum,omitempty"`
	LossProtection  float64                `json:"loss_protection,omitempty"`
	InvestingPeriod int                    `json:"investing_period,omitempty"`
}
