package common

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/cosmos/btcutil/bech32"
	ethcommon "github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"golang.org/x/crypto/ripemd160"
	"golang.org/x/crypto/sha3"
)

const tokensTTL = 1 * time.Hour

// AuthenticationDB is a cache for storing authentication tokens
type AuthenticationDB struct {
	cache *bigcache.BigCache
}

// NewAuthenticationDB creates a new AuthenticationDB
func NewAuthenticationDB() *AuthenticationDB {
	cache, _ := bigcache.New(context.Background(), bigcache.DefaultConfig(tokensTTL))

	return &AuthenticationDB{cache: cache}
}

// Init initializes an authentication token
func (a *AuthenticationDB) Init(chain Chain, pubkey []byte) (Token, error) {
	token, err := newToken(chain, pubkey)
	if err != nil {
		return Token{}, err
	}

	err = a.cache.Set(token.Id.String(), token.Bytes())
	if err != nil {
		return Token{}, err
	}

	return token, nil
}

// Verify verifies an authentication token
func (a *AuthenticationDB) Verify(id uuid.UUID, signature []byte) error {
	token, err := a.Get(id)
	if err != nil {
		return err
	}

	err = token.VerifyChallenge(signature)
	if err != nil {
		return err
	}

	token.Verified = true
	err = a.cache.Set(token.Id.String(), token.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves an authentication token
func (a *AuthenticationDB) Get(id uuid.UUID) (Token, error) {
	buf, err := a.cache.Get(id.String())
	if err != nil {
		return Token{}, err
	}

	var token Token
	err = json.Unmarshal(buf, &token)
	if err != nil {
		return Token{}, err
	}

	return token, nil
}

// Chain represents a blockchain network
type Chain string

const (
	ChainEthereum  Chain = "ETH"
	ChainThorchain Chain = "THOR"
)

// Token represents an authentication token
type Token struct {
	Id        uuid.UUID `json:"id,omitempty"`
	Chain     Chain     `json:"chain,omitempty"`
	PublicKey []byte    `json:"public_key,omitempty"`
	Address   string    `json:"address,omitempty"`
	IssuedAt  int64     `json:"issued_at,omitempty"`
	ExpiresAt int64     `json:"expires_at,omitempty"`
	Challenge string    `json:"challenge,omitempty"`
	Verified  bool      `json:"verified,omitempty"`
}

func newToken(chain Chain, pubkey []byte) (Token, error) {
	address, err := addressFromPubKey(chain, pubkey)
	if err != nil {
		return Token{}, fmt.Errorf("failed to get address from public key: %w", err)
	}

	return Token{
		Id:        uuid.New(),
		Chain:     chain,
		PublicKey: pubkey,
		Address:   address,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(tokensTTL).Unix(),
		Challenge: fmt.Sprintf("Authentication challenge: %s", base64.StdEncoding.EncodeToString(getRandomChallenge())),
		Verified:  false,
	}, nil
}

func addressFromPubKey(chain Chain, pubkey []byte) (string, error) {
	switch chain {
	case ChainEthereum:
		return generateEthereumAddress(pubkey)
	case ChainThorchain:
		return generateThorchainAddress(pubkey)
	default:
		return "", fmt.Errorf("unsupported chain: %s", chain)
	}
}

func generateEthereumAddress(pubkey []byte) (string, error) {
	var buf []byte

	hash := sha3.NewLegacyKeccak256()
	hash.Write(pubkey[1:]) // remove EC prefix 04
	buf = hash.Sum(nil)
	address := buf[12:]

	return ethcommon.BytesToAddress(address).String(), nil
}

func generateThorchainAddress(pubkey []byte) (string, error) {
	// Hash pubKeyBytes as: RIPEMD160(SHA256(public_key_bytes))
	pubKeySha256Hash := sha256.Sum256(pubkey)
	ripemd160hash := ripemd160.New()
	ripemd160hash.Write(pubKeySha256Hash[:])
	addressBytes := ripemd160hash.Sum(nil)

	// Convert addressBytes into a bech32 string
	address, err := toBech32("thor", addressBytes)
	if err != nil {
		return "", err
	}

	return address, nil
}

func toBech32(addrPrefix string, addrBytes []byte) (string, error) {
	converted, err := bech32.ConvertBits(addrBytes, 8, 5, true)
	if err != nil {
		return "", err
	}

	addr, err := bech32.Encode(addrPrefix, converted)
	if err != nil {
		return "", err
	}

	return addr, nil
}

func getRandomChallenge() []byte {
	var challenge [32]byte
	_, err := rand.Read(challenge[:])
	if err != nil {
		panic(err)
	}

	return challenge[:]
}

// Bytes returns the token as a byte slice
func (t Token) Bytes() []byte {
	buf, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}

	return buf
}

// VerifyChallenge verifies the challenge
func (t Token) VerifyChallenge(signature []byte) error {
	hash := sha3.NewLegacyKeccak256()
	hash.Write([]byte(t.Challenge))
	buf := hash.Sum(nil)

	sigPublicKey, err := ethcrypto.Ecrecover(buf, signature)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	if !bytes.Equal(sigPublicKey, t.PublicKey) {
		return fmt.Errorf("invalid signature")
	}

	signatureNoRecoverID := signature[:len(signature)-1] // remove recovery id
	if !ethcrypto.VerifySignature(t.PublicKey, buf, signatureNoRecoverID) {
		return fmt.Errorf("invalid signature")
	}

	return nil
}
