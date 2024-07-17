package cmd

import (
	"database/sql"
	"strings"

	"github.com/co-defi/api-server/app"
	"github.com/co-defi/api-server/app/commands"
	"github.com/co-defi/api-server/domain"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

// addPlanCmd represents the addPlan command
var addPlanCmd = &cobra.Command{
	Use:   "add-plan",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		connStr, _ := cmd.Flags().GetString("db")

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

		id := uuid.New()
		assets, _ := cmd.Flags().GetString("assets")
		security, _ := cmd.Flags().GetString("security")
		strategy, _ := cmd.Flags().GetString("strategy")
		quantum, _ := cmd.Flags().GetInt("quantum")
		LossProtection, _ := cmd.Flags().GetFloat64("loss-limit")
		investingPeriod, _ := cmd.Flags().GetInt("investing-period")
		err = app.Commands.CreateNewPlan.Handle(cmd.Context(), commands.CreateNewPlan{
			Id:              id,
			Assets:          strings.Split(assets, ","),
			Security:        domain.MultiSigWalletSecurity(security),
			Strategy:        domain.ProfitSharingStrategy(strategy),
			Quantum:         quantum,
			LossProtection:  LossProtection,
			InvestingPeriod: investingPeriod,
		})
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to create new plan")
		}

		logger.Info().Str("id", id.String()).Msg("new plan created")
	},
}

func init() {
	rootCmd.AddCommand(addPlanCmd)

	addPlanCmd.Flags().StringP("assets", "a", "", "Comma separated list of assets to include in the plan (e.g. THOR.RUNE,BTC.BTC)")
	addPlanCmd.Flags().StringP("security", "s", "2-2", "Security model to use (2of2, 2of3)")
	addPlanCmd.Flags().StringP("strategy", "t", "equal_share", "Strategy to use (equal-share, custom)")
	addPlanCmd.Flags().IntP("quantum", "q", 100, "Quantum value of each share measured in $")
	addPlanCmd.Flags().Float64P("loss-limit", "l", 0.1, "Loss limit")
	addPlanCmd.Flags().IntP("investing-period", "i", 1, "Investing period in weeks")
}
