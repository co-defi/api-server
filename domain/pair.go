package domain

import (
	"time"

	"github.com/hallgren/eventsourcing"
)

// Pair is the aggregate root for a pair of participates going through a liquidity providing process for a pair of crypto assets.
// We create a Pair for an individual participant for a plan with validated account address that has more than the required quantum of assets in $.
// After that the Pair is thrown in a queue or pool of Pairs with the ShareValue same as the quantum of the selected plan.
// The Pair is then matched with another participant who has interest in the same plan but with the counterpart asset.
// The participants then go through creating a shared wallet with the required security method and series of steps for the liquidity providing process.
type Pair struct {
	eventsourcing.AggregateRoot
	Assets              []string         `json:"assets,omitempty"`
	ParticipantsAddress []string         `json:"participants_address,omitempty"`
	ShareValue          int              `json:"share_value,omitempty"`
	Wallet              *MultisigWallet  `json:"wallet,omitempty"`
	Step1               *Asset1Assurance `json:"step1,omitempty"`
	Step2               *Asset1Transfer  `json:"step2,omitempty"`
	Step3               *Asset2Assurance `json:"step3,omitempty"`
	Step4               *Asset2Transfer  `json:"step4,omitempty"`
	Step5               *LP              `json:"step5,omitempty"`
	Deadline            time.Time        `json:"deadline,omitempty"`
	AutoWithdrawal      *SignedTx        `json:"auto_withdrawal,omitempty"`
}

// Register implements aggregate.Register
func (p *Pair) Register(r eventsourcing.RegisterFunc) {}

// Transition implements aggregate.Transition
func (p *Pair) Transition(event eventsourcing.Event) {}

// MultisigWallet is the shared wallet for the pair of participants
type MultisigWallet struct {
	Security  Security `json:"security,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
	// Other fields for Vaultisig internal stuff...
}

// Asset1Assurance is the first step of the liquidity providing process
// where the both participants sign a transaction to transfer the ShareValue amount of Assets[0] to the first participant's address with nonce 0
// in case of a dispute.
type Asset1Assurance struct {
	Tx SignedTx `json:"tx,omitempty"`
}

// Asset1Transfer is the second step of the liquidity providing process
// where the first participant transfers the ShareValue amount of Assets[0] to the shared wallet.
type Asset1Transfer struct {
	TxHash string `json:"tx_hash,omitempty"`
}

// Asset2Assurance is the third step of the liquidity providing process
// where the both participants sign a transaction to transfer the ShareValue amount of Assets[1] to the second participant's address with nonce 1
// in case of a dispute.
type Asset2Assurance struct {
	Tx SignedTx `json:"tx,omitempty"`
}

// Asset2Transfer is the fourth step of the liquidity providing process
// where the second participant transfers the ShareValue amount of Assets[1] to the shared wallet.
type Asset2Transfer struct {
	TxHash string `json:"tx_hash,omitempty"`
}

// LP is the fifth step of the liquidity providing process
// where both participants sign a transaction to LP the assets in the desired pool.
type LP struct {
	Asset1TxHash string `json:"asset1_tx_hash,omitempty"`
	Asset2TxHash string `json:"asset2_tx_hash,omitempty"`
}

// SignedTx is the type for the transactions that are signed by the participants
type SignedTx struct {
	Nonce     int    `json:"nonce,omitempty"`
	Tx        []byte `json:"tx,omitempty"`
	Signature []byte `json:"signature,omitempty"`
}
