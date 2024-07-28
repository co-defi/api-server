package common

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/cosmos/btcutil/bech32"
	ethaccounts "github.com/ethereum/go-ethereum/accounts"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"golang.org/x/crypto/ripemd160"
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
func (a *AuthenticationDB) Init(chain Chain, address string) (Token, error) {
	token, err := newToken(chain, address)
	if err != nil {
		return Token{}, err
	}

	err = a.cache.Set(token.Id.String(), token.Bytes())
	if err != nil {
		return Token{}, err
	}

	return token, nil
}

var (
	ErrAuthenticationExpired            = NewError("auth_expired", "authentication expired or not found")
	ErrAuthenticationVerificationFailed = NewError("auth_verification_failed", "authentication verification failed")
)

// Verify verifies an authentication token
func (a *AuthenticationDB) Verify(id uuid.UUID, signature []byte) error {
	token, err := a.Get(id)
	if err != nil {
		return ErrAuthenticationExpired
	}

	err = token.VerifyChallenge(signature)
	if err != nil {
		return ErrAuthenticationVerificationFailed.IncludeMeta(map[string]interface{}{"error": err.Error()})
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
type Chain = string

const (
	ChainEthereum  Chain = "ETH"
	ChainThorchain Chain = "THOR"
)

// Token represents an authentication token
type Token struct {
	Id        uuid.UUID `json:"id,omitempty"`
	Chain     Chain     `json:"chain,omitempty"`
	Address   string    `json:"address,omitempty"`
	IssuedAt  int64     `json:"issued_at,omitempty"`
	ExpiresAt int64     `json:"expires_at,omitempty"`
	Challenge string    `json:"challenge,omitempty"`
	Verified  bool      `json:"verified,omitempty"`
}

var (
	ErrAuthenticationFailed      = NewError("auth_failed", "authentication failed")
	ErrAuthenticationNotVerified = NewError("auth_not_verified", "authentication not verified")
)

// ExtractTokenFromHttp extracts an authentication token from the HTTP request Authorization header as a Bearer token
func (db *AuthenticationDB) ExtractTokenFromHttp(r *http.Request) (Token, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return Token{}, ErrAuthenticationFailed
	}

	hParts := strings.Split(h, " ")
	if len(hParts) != 2 || hParts[0] != "Bearer" {
		return Token{}, ErrAuthenticationFailed
	}

	tokenId, err := uuid.Parse(hParts[1])
	if err != nil {
		return Token{}, ErrAuthenticationFailed
	}

	token, err := db.Get(tokenId)
	if err != nil {
		return Token{}, ErrAuthenticationExpired
	}

	if token.ExpiresAt < time.Now().Unix() {
		return Token{}, ErrAuthenticationExpired
	}

	if !token.Verified {
		return Token{}, ErrAuthenticationNotVerified
	}

	return token, nil
}

var ErrInvalidPublicKey = NewError("invalid_public_key", "failed to generate address for this pair of chain and public key")

func newToken(chain Chain, address string) (Token, error) {
	return Token{
		Id:        uuid.New(),
		Chain:     chain,
		Address:   address,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(tokensTTL).Unix(),
		Challenge: fmt.Sprintf("Authentication Challenge: %s", base64.StdEncoding.EncodeToString(getRandomChallenge())),
		Verified:  false,
	}, nil
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
	switch t.Chain {
	case ChainEthereum:
		return verifyEthereumChallenge(t.Address, t.Challenge, signature)
	case ChainThorchain:
		// TODO: implement thorchain signature verification
		return nil
	}

	return nil
}

func verifyEthereumChallenge(address, challenge string, signature []byte) error {
	hash := ethaccounts.TextHash([]byte(challenge))
	signature[ethcrypto.RecoveryIDOffset] -= 27 // transform V from 27/28 to 0/1
	pub, err := ethcrypto.SigToPub(hash, signature)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	if address != ethcrypto.PubkeyToAddress(*pub).String() {
		return fmt.Errorf("invalid public key address")
	}

	signatureNoRecoverID := signature[:len(signature)-1] // remove recovery id
	if !ethcrypto.VerifySignature(ethcrypto.FromECDSAPub(pub), hash, signatureNoRecoverID) {
		return fmt.Errorf("invalid signature")
	}

	return nil
}

const thorchainBech32Prefix = "thor"

func generateThorchainAddress(pubkey []byte) (string, error) {
	return generateBech32Address(thorchainBech32Prefix, pubkey)
}

func generateBech32Address(hrp string, pubkey []byte) (string, error) {
	pk, err := ethcrypto.UnmarshalPubkey(pubkey)
	if err != nil {
		return "", err
	}
	compressedPubKey := ethcrypto.CompressPubkey(pk)

	// Hash pubKeyBytes as: RIPEMD160(SHA256(public_key_bytes))
	sha256Hash := sha256.Sum256(compressedPubKey)
	ripemd160hash := ripemd160.New()
	ripemd160hash.Write(sha256Hash[:])
	addressBytes := ripemd160hash.Sum(nil)

	// Convert addressBytes into a bech32 string
	address, err := toBech32(hrp, addressBytes)
	if err != nil {
		return "", err
	}

	return address, nil
}
