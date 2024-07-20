package ports

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4/middleware"

	"github.com/co-defi/api-server/app"
	"github.com/co-defi/api-server/app/commands"
	"github.com/co-defi/api-server/common"
	"github.com/co-defi/api-server/domain"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
)

// HttpServer is a HTTP server that listens for incoming REST requests
// and routes them to the appropriate command and query handlers.
type HttpServer struct {
	app    *app.Application
	echo   *echo.Echo
	logger zerolog.Logger
}

// NewHttpServer creates a new HTTP server
func NewHttpServer(a *app.Application) *HttpServer {
	e := echo.New()
	e.Use(middleware.CORS())
	e.Use(middleware.Logger())
	s := HttpServer{
		app:    a,
		echo:   e,
		logger: zerolog.Nop(),
	}
	s.registerRoutes()
	s.echo.HTTPErrorHandler = s.handleError

	return &s
}

func (s *HttpServer) registerRoutes() {
	s.echo.GET("/plans", s.getPlans)
	s.echo.GET("/plan:id", s.getPlan)

	s.echo.POST(("/pairs"), s.createOrMatchPair)
	s.echo.GET("/pairs/:id", s.getPair)
	s.echo.POST("/pairs/:id/confirm-wallet", s.confirmPairWallet)
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

func (s *HttpServer) getPlan(c echo.Context) error {
	id := c.Param("id")
	if id == "" {
		return c.JSON(http.StatusBadRequest, common.Error{Code: "invalid_id", Message: "id is required"})
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

type createOrMatchPairRequest struct {
	PlanId             string         `json:"plan_id"`
	ParticipantAsset   domain.Asset   `json:"participant_asset"`
	ParticipantAddress domain.Address `json:"participant_address"`
}

type createOrMatchPairResponse struct {
	Id string `json:"id"`
}

func (s *HttpServer) createOrMatchPair(c echo.Context) error {
	var req createOrMatchPairRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	pairId, err := s.app.Commands.CreateOrMatchPair.Handle(c.Request().Context(), commands.CreateOrMatchPair{
		PlanId:             req.PlanId,
		ParticipantAsset:   req.ParticipantAsset,
		ParticipantAddress: req.ParticipantAddress,
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

	return c.JSON(http.StatusOK, pair)
}

type confirmPairWalletRequest struct {
	ParticipantAsset     domain.Asset                    `json:"participant_asset,omitempty"`
	ParticipantPublicKey string                          `json:"participant_public_key,omitempty"`
	WalletAddresses      map[domain.Asset]domain.Address `json:"wallet_addresses,omitempty"`
}

func (s *HttpServer) confirmPairWallet(c echo.Context) error {
	var req confirmPairWalletRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	_, err := s.app.Commands.ConfirmPairWallet.Handle(c.Request().Context(), commands.ConfirmPairWallet{
		PairId:               c.Param("id"),
		ParticipantAsset:     req.ParticipantAsset,
		ParticipantPublicKey: req.ParticipantPublicKey,
		WalletAddresses:      req.WalletAddresses,
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
