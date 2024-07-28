package domain

import (
	"time"

	"github.com/hallgren/eventsourcing"
)

const EmptyAddress = ""

// Pair is the aggregate root for a pair of participates going through a liquidity providing process for a pair of crypto assets.
// We create a Pair for an individual participant for a plan with validated account address that has more than the required quantum of assets in $.
// After that the Pair is thrown in a queue or pool of Pairs with the ShareValue same as the quantum of the selected plan.
// The Pair is then matched with another participant who has interest in the same plan but with the counterpart asset.
// The participants then go through creating a shared wallet with the required security method and series of steps for the liquidity providing process.
type Pair struct {
	eventsourcing.AggregateRoot
	Status                PairStatus             `json:"status,omitempty"`
	Assets                []Asset                `json:"assets,omitempty"`
	ParticipantsAddress   map[Asset]Address      `json:"participants_address,omitempty"`
	ShareValue            int                    `json:"share_value,omitempty"`
	InvestingPeriod       int                    `json:"investing_period,omitempty"`
	WalletSecurity        MultiSigWalletSecurity `json:"wallet_security,omitempty"`
	ProfitSharingStrategy ProfitSharingStrategy  `json:"profit_sharing_strategy,omitempty"`
	LossProtection        float64                `json:"loss_protection,omitempty"`
	Wallet                *MultisigWallet        `json:"wallet,omitempty"`
	Assurances            map[Asset][]SignedTx   `json:"assurances,omitempty"`
	Deposits              map[Asset]TxHash       `json:"deposits,omitempty"`
	WithdrawTx            *SignedTx              `json:"withdraw_tx,omitempty"`
	LP                    map[Asset]TxHash       `json:"lp,omitempty"`
	Deadline              time.Time              `json:"deadline,omitempty"`
	WithdrawnTx           *TxHash                `json:"withdrawn_tx,omitempty"`
}

// Register implements aggregate.Register
func (p *Pair) Register(r eventsourcing.RegisterFunc) {
	r(
		&PairCreated{},
		&PairStatusChanged{},
		&PairMatched{},
		&WalletAddressConfirmed{},
		&AssetAssuranceSigned{},
		&AssetDeposited{},
		&WithdrawTxSigned{},
		&LPDone{},
		&Withdrawn{},
	)
}

// Transition implements aggregate.Transition
func (p *Pair) Transition(event eventsourcing.Event) {
	switch e := event.Data().(type) {
	case *PairCreated:
		p.applyPairCreated(e)
	case *PairStatusChanged:
		p.applyPairStatusChanged(e)
	case *PairMatched:
		p.applyPairMatched(e)
	case *WalletAddressConfirmed:
		p.applyWalletAddressConfirmed(e)
	case *AssetAssuranceSigned:
		p.applyAssetAssuranceSigned(e)
	case *AssetDeposited:
		p.applyAssetDeposited(e)
	case *WithdrawTxSigned:
		p.applyWithdrawTxSigned(e)
	case *LPDone:
		p.applyLPDone(e)
	case *Withdrawn:
		p.applyWithdrawn(e)
	}
}

func (p *Pair) applyPairCreated(e *PairCreated) {
	p.Assets = []Asset{e.ParticipantAsset, e.SecondaryAsset}
	p.ParticipantsAddress = map[Asset]Address{e.ParticipantAsset: e.ParticipantAddress}
	p.ShareValue = e.ShareValue
	p.InvestingPeriod = e.InvestingPeriod
	p.WalletSecurity = e.WalletSecurity
	p.ProfitSharingStrategy = e.ProfitSharingStrategy
	p.LossProtection = e.LossProtection
}

func (p *Pair) applyPairStatusChanged(e *PairStatusChanged) {
	p.Status = e.Status
}

func (p *Pair) applyPairMatched(e *PairMatched) {
	p.Wallet = &MultisigWallet{
		PublicKeys:    make(map[Asset]string),
		EncryptionKey: e.WalletEncryptionKey,
		HexChainCode:  e.WalletHexChainCode,
	}
	p.ParticipantsAddress[p.Assets[1]] = e.ParticipantAddress
}

func (p *Pair) applyWalletAddressConfirmed(e *WalletAddressConfirmed) {
	p.Wallet.Addresses = e.WalletAddresses
	p.Wallet.PublicKeys[e.ParticipantAsset] = e.PublicKey
}

func (p *Pair) applyAssetAssuranceSigned(e *AssetAssuranceSigned) {
	if p.Assurances == nil {
		p.Assurances = make(map[Asset][]SignedTx)
	}

	if _, ok := p.Assurances[e.Asset]; !ok {
		p.Assurances[e.Asset] = make([]SignedTx, 0)
	}
	p.Assurances[e.Asset] = append(p.Assurances[e.Asset], e.Tx)
}

func (p *Pair) applyAssetDeposited(e *AssetDeposited) {
	if p.Deposits == nil {
		p.Deposits = make(map[Asset]TxHash)
	}

	p.Deposits[e.Asset] = e.TxHash
}

func (p *Pair) applyWithdrawTxSigned(e *WithdrawTxSigned) {
	p.WithdrawTx = &e.Tx
}

func (p *Pair) applyLPDone(e *LPDone) {
	if p.LP == nil {
		p.LP = make(map[Asset]TxHash)
	}

	p.LP[e.Asset] = e.TxHash

	p.Deadline = e.Deadline
}

func (p *Pair) applyWithdrawn(e *Withdrawn) {
	p.WithdrawnTx = &e.TxHash
}

// HasAsset checks if the pair has the asset
func (p Pair) HasAsset(asset Asset) bool {
	for _, a := range p.Assets {
		if a == asset {
			return true
		}
	}

	return false
}

// HasParticipant checks if the pair has the participant
func (p Pair) HasParticipant(address Address) bool {
	for _, addr := range p.ParticipantsAddress {
		if addr == address {
			return true
		}
	}

	return false
}

// AssetOfParticipant returns the asset of the participant
func (p Pair) AssetOfParticipant(address Address) Asset {
	for asset, addr := range p.ParticipantsAddress {
		if addr == address {
			return asset
		}
	}

	return ""
}

// HasAssurancesForAsset checks if the pair has assurances for the asset
func (p Pair) HasAssurancesForAsset(asset Asset) bool {
	_, ok := p.Assurances[asset]
	return ok
}

// HasDepositForAsset checks if the pair has deposits for the asset
func (p Pair) HasDepositForAsset(asset Asset) bool {
	_, ok := p.Deposits[asset]
	return ok
}

// HasLPForAsset checks if the pair has liquidity providing for the asset
func (p Pair) HasLPForAsset(asset Asset) bool {
	_, ok := p.LP[asset]
	return ok
}

// PairStatus is the type for the status of the pair
type PairStatus string

const (
	PairStatusWaiting            PairStatus = "waiting"
	PairStatusWalletConformation PairStatus = "wallet_conformation"
	PairStatusAssurance          PairStatus = "assurance"
	PairStatusDeposit            PairStatus = "deposit"
	PairStatusPreSignWithdrawal  PairStatus = "pre_sign_withdrawal"
	PairStatusLP                 PairStatus = "lp"
	PairStatusWithdrawn          PairStatus = "withdrawn"
	PairStatusInvalid            PairStatus = "invalid"
)

// Asset is the type for the assets in the pair
type Asset = string

// Address is the type for the participant's address
type Address = string

// MultisigWallet is the shared wallet for the pair of participants
type MultisigWallet struct {
	PublicKeys    map[Asset]string  `json:"public_keys,omitempty"`
	Addresses     map[Asset]Address `json:"addresses,omitempty"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	HexChainCode  string            `json:"hex_chain_code,omitempty"`
}

func (w *MultisigWallet) AreAddressesEqual(addresses map[Asset]Address) bool {
	for asset, address := range addresses {
		if w.Addresses[asset] != address {
			return false
		}
	}

	return true
}

// SignedTx is the type for the transactions that are signed by the participants
type SignedTx struct {
	Nonce     int    `json:"nonce"`
	Tx        []byte `json:"tx"`
	Signature []byte `json:"signature"`
}

// TxHash is the type for the transaction hash
type TxHash = string

// PairCreated is the event for creating a new pair for the first time.
type PairCreated struct {
	ParticipantAsset      Asset                  `json:"participant_asset,omitempty"`
	ParticipantAddress    Address                `json:"participant_address,omitempty"`
	SecondaryAsset        Asset                  `json:"secondary_asset,omitempty"`
	ShareValue            int                    `json:"share_value,omitempty"`
	InvestingPeriod       int                    `json:"investing_period,omitempty"`
	WalletSecurity        MultiSigWalletSecurity `json:"wallet_security,omitempty"`
	ProfitSharingStrategy ProfitSharingStrategy  `json:"profit_sharing_strategy,omitempty"`
	LossProtection        float64                `json:"loss_protection,omitempty"`
}

// PairStatusChanged is the event for changing the status of the pair.
type PairStatusChanged struct {
	Status PairStatus `json:"status,omitempty"`
}

// PairMatched is the event for matching a pair with another participant.
type PairMatched struct {
	ParticipantAddress  Address `json:"participant_address,omitempty"`
	WalletEncryptionKey string  `json:"wallet_encryption_key,omitempty"`
	WalletHexChainCode  string  `json:"wallet_hex_chain_code,omitempty"`
	KeygenMsg           string  `json:"keygen_msg,omitempty"`
}

// WalletAddressConfirmed is the event for confirming the shared wallet's addresses by the participants.
type WalletAddressConfirmed struct {
	ParticipantAsset Asset             `json:"participant,omitempty"`
	PublicKey        string            `json:"public_key,omitempty"`
	WalletAddresses  map[Asset]Address `json:"addresses,omitempty"`
}

// AssetAssuranceSigned is the event for signing the assurance transaction for the asset.
type AssetAssuranceSigned struct {
	Asset Asset    `json:"asset,omitempty"`
	Tx    SignedTx `json:"tx,omitempty"`
}

// AssetDeposited is the event for signing the transfer transaction for the asset.
type AssetDeposited struct {
	Asset  Asset  `json:"asset,omitempty"`
	TxHash TxHash `json:"tx_hash,omitempty"`
}

// WithdrawTxSigned is the event for signing the withdrawal transaction.
type WithdrawTxSigned struct {
	Tx SignedTx `json:"tx,omitempty"`
}

// LPDone is the event for when the liquidity providing is done.
type LPDone struct {
	Asset    Asset     `json:"asset,omitempty"`
	TxHash   TxHash    `json:"tx_hash,omitempty"`
	Deadline time.Time `json:"deadline,omitempty"`
}

// Withdrawn is the event for when the withdrawal is done.
type Withdrawn struct {
	TxHash TxHash `json:"tx_hash,omitempty"`
}
