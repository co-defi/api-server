package ports

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4/middleware"

	"github.com/co-defi/api-server/app"
	"github.com/co-defi/api-server/app/commands"
	"github.com/co-defi/api-server/app/queries"
	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
)

// HttpServer is a HTTP server that listens for incoming REST requests
// and routes them to the appropriate command and query handlers.
type HttpServer struct {
	app    *app.Application
	authDB *common.AuthenticationDB
	echo   *echo.Echo
	logger zerolog.Logger
}

// NewHttpServer creates a new HTTP server
func NewHttpServer(a *app.Application) *HttpServer {
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())
	e.Use(middleware.CORS())

	s := HttpServer{
		app:    a,
		authDB: common.NewAuthenticationDB(),
		echo:   e,
		logger: zerolog.Nop(),
	}
	s.registerRoutes()
	s.echo.HTTPErrorHandler = s.handleError

	return &s
}

func (s *HttpServer) registerRoutes() {
	s.echo.POST("/auth/init", s.initAuth)
	s.echo.POST("/auth/verify", s.verifyAuth)

	s.echo.GET("/plans", s.getPlans)
	s.echo.GET("/plan/:id", s.getPlan)

	s.echo.POST(("/pairs"), s.createOrMatchPair)
	s.echo.GET("/pairs/:id", s.getPair)
	s.echo.GET("/pairs", s.getPairs)
	s.echo.POST("/pairs/:id/confirm-wallet", s.confirmPairWallet)
	s.echo.POST("/pairs/:id/assurances", s.setPairAssurances)
	s.echo.POST("/pairs/:id/deposits", s.addDeposit)
	s.echo.POST("/pairs/:id/sign-withdraw", s.signWithdrawal)
	s.echo.POST("/pairs/:id/submit-lp", s.submitLP)
	s.echo.POST("/pairs/:id/submit-withdrawal", s.submitWithdrawal)
}

type initAuthRequest struct {
	Chain  common.Chain `json:"chain"`
	PubKey []byte       `json:"pub_key"`
}

func (s *HttpServer) initAuth(c echo.Context) error {
	var req initAuthRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	token, err := s.authDB.Init(req.Chain, []byte(req.PubKey))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, token)
}

type verifyAuthRequest struct {
	Id        uuid.UUID `json:"id"`
	Signature []byte    `json:"signature"`
}

func (s *HttpServer) verifyAuth(c echo.Context) error {
	var req verifyAuthRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	err := s.authDB.Verify(req.Id, req.Signature)
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type plan struct {
	Id              string         `json:"id"`
	Name            string         `json:"name"`
	Assets          []domain.Asset `json:"assets"`
	Security        string         `json:"security"`
	Strategy        string         `json:"strategy"`
	Quantum         int            `json:"quantum"`
	LossProtection  float64        `json:"loss_protection"`
	InvestingPeriod int            `json:"time_frame"`
	APR             float64        `json:"APR"`
}

func (s *HttpServer) getPlans(c echo.Context) error {
	plans, err := s.app.Queries.Plans.All(c.Request().Context())
	if err != nil {
		return err
	}
	response := make([]plan, len(plans))
	for i, p := range plans {
		response[i] = plan{
			Id:              p.Id,
			Name:            "Basic Low Risk Plan", // TODO: "Basic Low Risk Plan" is a hardcoded value, it should be fetched from the database
			Assets:          p.Assets,
			Security:        string(p.Security),
			Strategy:        string(p.Strategy),
			Quantum:         p.Quantum,
			LossProtection:  p.LossProtection,
			InvestingPeriod: p.InvestingPeriod,
			APR:             0.15,
		}
	}

	return c.JSON(http.StatusOK, response)
}

var ErrInvalidPlanId = common.NewError("invalid_plan_id", "plan id is required")

func (s *HttpServer) getPlan(c echo.Context) error {
	id := c.Param("id")
	if id == "" {
		return ErrInvalidPlanId
	}

	p, err := s.app.Queries.Plans.Get(c.Request().Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, plan{
		Id:              p.Id,
		Name:            "Basic Low Risk Plan", // TODO: "Basic Low Risk Plan" is a hardcoded value, it should be fetched from the database
		Assets:          p.Assets,
		Security:        string(p.Security),
		Strategy:        string(p.Strategy),
		Quantum:         p.Quantum,
		LossProtection:  p.LossProtection,
		InvestingPeriod: p.InvestingPeriod,
		APR:             0.15,
	})
}

var ErrForbidden = common.NewError("forbidden", "forbidden content access")

type createOrMatchPairRequest struct {
	PlanId           string       `json:"plan_id"`
	ParticipantAsset domain.Asset `json:"participant_asset"`
}

type createOrMatchPairResponse struct {
	Id string `json:"id"`
}

func (s *HttpServer) createOrMatchPair(c echo.Context) error {
	var req createOrMatchPairRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}
	if !strings.HasPrefix(req.ParticipantAsset, auth.Chain) {
		return ErrForbidden
	}

	pairId, err := s.app.Commands.CreateOrMatchPair.Handle(c.Request().Context(), commands.CreateOrMatchPair{
		PlanId:             req.PlanId,
		ParticipantAsset:   req.ParticipantAsset,
		ParticipantAddress: auth.Address,
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, createOrMatchPairResponse{Id: pairId})
}

func (s *HttpServer) getPair(c echo.Context) error {
	pair, err := s.app.Queries.Pairs.Get(c.Request().Context(), c.Param("id"))
	if err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}
	if !pairHasAddress(pair, auth.Address) {
		return ErrForbidden
	}

	return c.JSON(http.StatusOK, pair)
}

func pairHasAddress(pair *queries.Pair, address string) bool {
	for _, p := range pair.ParticipantAddresses {
		if p == address {
			return true
		}
	}

	return false
}

var ErrInvalidAddress = common.NewError("invalid_address", "address is required")

func (s *HttpServer) getPairs(c echo.Context) error {
	planId := c.QueryParam("plan_id")
	if err := uuid.Validate(planId); err != nil {
		return ErrInvalidPlanId.IncludeMeta(map[string]interface{}{"plan_id": err})
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	plan, err := s.app.Queries.Plans.Get(c.Request().Context(), planId)
	if err != nil {
		return err
	}

	pairs, err := s.app.Queries.Pairs.Find(
		c.Request().Context(),
		nil,
		plan.Assets,
		false,
		[]domain.Address{auth.Address},
		&plan.Quantum,
		&plan.InvestingPeriod,
		&plan.Security,
		&plan.Strategy,
		&plan.LossProtection,
	)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, pairs)
}

type confirmPairWalletRequest struct {
	ParticipantPublicKey string                          `json:"participant_public_key,omitempty"`
	WalletAddresses      map[domain.Asset]domain.Address `json:"wallet_addresses,omitempty"`
}

func (s *HttpServer) confirmPairWallet(c echo.Context) error {
	var req confirmPairWalletRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.ConfirmPairWallet.Handle(c.Request().Context(), commands.ConfirmPairWallet{
		PairId:               c.Param("id"),
		ParticipantAddress:   auth.Address,
		ParticipantPublicKey: req.ParticipantPublicKey,
		WalletAddresses:      req.WalletAddresses,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type setPairAssurancesRequest struct {
	Asset      domain.Asset      `json:"asset,omitempty"`
	Assurances []domain.SignedTx `json:"assurances,omitempty"`
}

func (s *HttpServer) setPairAssurances(c echo.Context) error {
	var req setPairAssurancesRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.SetPairAssurances.Handle(c.Request().Context(), commands.SetPairAssurances{
		PairId:             c.Param("id"),
		ParticipantAddress: auth.Address,
		Asset:              req.Asset,
		Assurances:         req.Assurances,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type addDepositRequest struct {
	Asset  domain.Asset  `json:"asset,omitempty"`
	TxHash domain.TxHash `json:"tx_hash,omitempty"`
}

func (s *HttpServer) addDeposit(c echo.Context) error {
	var req addDepositRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.AddDeposit.Handle(c.Request().Context(), commands.AddDeposit{
		PairId:             c.Param("id"),
		ParticipantAddress: auth.Address,
		Asset:              req.Asset,
		TxHash:             req.TxHash,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type signWithdrawalRequest struct {
	Tx domain.SignedTx `json:"tx,omitempty"`
}

func (s *HttpServer) signWithdrawal(c echo.Context) error {
	var req signWithdrawalRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.SignWithdrawal.Handle(c.Request().Context(), commands.SignWithdrawal{
		PairId:             c.Param("id"),
		ParticipantAddress: auth.Address,
		Tx:                 req.Tx,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type submitLPRequest struct {
	Asset  domain.Asset  `json:"asset,omitempty"`
	TxHash domain.TxHash `json:"tx_hash,omitempty"`
}

func (s *HttpServer) submitLP(c echo.Context) error {
	var req submitLPRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.SubmitLP.Handle(c.Request().Context(), commands.SubmitLP{
		PairId:             c.Param("id"),
		ParticipantAddress: auth.Address,
		Asset:              req.Asset,
		TxHash:             req.TxHash,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

type submitWithdrawalRequest struct {
	TxHash domain.TxHash `json:"tx_hash,omitempty"`
}

func (s *HttpServer) submitWithdrawal(c echo.Context) error {
	var req submitWithdrawalRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	auth, err := s.authDB.ExtractTokenFromHttp(c.Request())
	if err != nil {
		return err
	}

	_, err = s.app.Commands.SubmitWithdrawal.Handle(c.Request().Context(), commands.SubmitWithdrawal{
		PairId:             c.Param("id"),
		ParticipantAddress: &auth.Address,
		TxHash:             req.TxHash,
	})
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

func (s *HttpServer) handleError(err error, c echo.Context) {
	var (
		commonErr     *common.Error
		validationErr validator.ValidationErrors
	)
	if errors.As(err, &commonErr) {
		c.JSON(convertCodeToHttpStatus(commonErr.Code), commonErr)
	} else if errors.As(err, &validationErr) {
		c.JSON(http.StatusBadRequest, common.ErrorFromValidationErrors(validationErr))
	} else {
		s.echo.DefaultHTTPErrorHandler(err, c)
	}

	if c.Response().Status == http.StatusInternalServerError {
		s.logger.Error().Err(err).Msg("internal server error")
	}
}

func convertCodeToHttpStatus(code string) int {
	switch {
	case strings.Contains(code, "not_found"):
		return http.StatusNotFound
	case strings.Contains(code, "invalid"):
		return http.StatusBadRequest
	case strings.Contains(code, "already"):
		return http.StatusBadRequest
	case strings.Contains(code, "auth"):
		return http.StatusUnauthorized
	case strings.Contains(code, "forbidden"):
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

// WithLogger sets the logger for the server
func (s *HttpServer) WithLogger(logger zerolog.Logger) {
	s.logger = logger
}

// Start starts the HTTP server
func (s *HttpServer) Start(addr string) error {
	return s.echo.Start(addr)
}
