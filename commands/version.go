package commands

import (
	"github.com/spf13/cobra"
	jww "github.com/spf13/jwalterweatherman"
)

var versionCommand = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of velocity",
	Long:  `All software has versions. This is velocity's.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		printVelocityVersion()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCommand)
}

func printVelocityVersion() {
	jww.FEEDBACK.Println("0.1")
}
