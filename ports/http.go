package ports

import (
	"github.com/labstack/echo/v4/middleware"
	"net/http"
	"strings"

	"github.com/co-defi/api-server/app"
	"github.com/co-defi/api-server/common"
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

// WithLogger sets the logger for the server
func (s *HttpServer) WithLogger(logger zerolog.Logger) {
	s.logger = logger
}

func (s *HttpServer) registerRoutes() {
	s.echo.GET("/plans", s.getPlans)
}

func (s *HttpServer) getPlans(c echo.Context) error {
	plans, err := s.app.Queries.Plans.All()
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, plans)
}

func (s *HttpServer) handleError(err error, c echo.Context) {
	switch err := err.(type) {
	case *common.Error:
		c.JSON(convertCodeToHttpStatus(err.Code), err)
	default:
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

// Start starts the HTTP server
func (s *HttpServer) Start(addr string) error {
	return s.echo.Start(addr)
}
