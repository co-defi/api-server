package cmd

import (
	"github.com/co-defi/api-server/common"
	"github.com/spf13/cobra"
)

// resetProjections represents the reset-projections command
var resetProjectionsCmd = &cobra.Command{
	Use:   "reset-projections",
	Short: "Reset all projections",
	Long:  `This command resets all projections in the database.`,
	Run: func(cmd *cobra.Command, args []string) {
		db, err := prepareDB(cmd.Flags())
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to open database")
		}
		defer db.Close()

		if err := common.ResetAllProjections(db); err != nil {
			logger.Fatal().Err(err).Msg("failed to reset projections")
		}
	},
}

func init() {
	rootCmd.AddCommand(resetProjectionsCmd)
}
