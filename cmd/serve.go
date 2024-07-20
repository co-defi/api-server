package cmd

import (
	"database/sql"

	"github.com/co-defi/api-server/app"
	"github.com/co-defi/api-server/ports"
	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve the HTTP APIs",
	Long: `This command starts the HTTP server that serves the APIs.
It listens on the specified port and connects to the database using the provided connection string.`,
	Run: func(cmd *cobra.Command, args []string) {
		connStr, _ := cmd.Flags().GetString("db")
		port, _ := cmd.Flags().GetString("port")

		db, err := sql.Open("sqlite3", connStr)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to open database")
		}
		defer db.Close()

		app, err := app.NewApplication(db)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to create application instance")
		}
		app.WithLogger(logger)
		app.StartProjections()
		defer app.StopProjections()

		server := ports.NewHttpServer(app)
		server.WithLogger(logger)

		if err := server.Start(port); err != nil {
			logger.Fatal().Err(err).Msg("failed to start server")
		}
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringP("port", "p", ":8080", "Port to listen on")
}
