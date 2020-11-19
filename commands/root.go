package commands

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dragosv/velocity/db"
	"github.com/jinzhu/gorm"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	jww "github.com/spf13/jwalterweatherman"
	"github.com/spf13/viper"
	"log"
	"os"
	"strconv"
	"time"
)

type jsonRecord struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	LoadAmount string    `json:"load_amount"`
	Time       time.Time `json:"time"`
}

type Record struct {
	ID         uint
	CustomerID uint
	LoadAmount float64
	Time       time.Time
}

type Response struct {
	ID         uint `json:"id",omitempty`
	CustomerID uint `json:"customer_id",omitempty`
	Accepted   bool `json:"accepted",omitempty`
}

type jsonResponse struct {
	ID         string `json:"id",omitempty`
	CustomerID string `json:"customer_id",omitempty`
	Accepted   bool   `json:"accepted",omitempty`
}

var (
	// Used for flags.
	cfgFile            string
	source             string
	databaseDialect    string
	databaseConnection string
	destination        string
	fs                 afero.Fs
	database           *gorm.DB

	rootCmd = &cobra.Command{
		Use:   "velocity",
		Short: "Velocity Limits Command Line Interface",
		Long:  `Velocity Limits is a program that accepts or declines attempts to load funds into customers' accounts in real-time.`,

		RunE: func(cmd *cobra.Command, args []string) error {
			fs = afero.NewOsFs()

			var err error

			database, err = openDatabase(databaseDialect, databaseConnection)
			if err != nil {
				return errors.New("failed to connect database " + err.Error())
			}

			return runRootCommand(source, destination)
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "c", "config file (default is $HOME/.velocity)")

	rootCmd.Flags().StringVarP(&source, "source", "s", "input.txt", "Source file to read from")
	rootCmd.Flags().StringVarP(&destination, "destination", "d", "output.txt", "Destination file to write to")
	rootCmd.Flags().StringVarP(&databaseDialect, "dialect", "", "sqlite3", "Database dialect")
	rootCmd.Flags().StringVarP(&databaseConnection, "connection", "", "file:velocity.sqlite", "Database connection string")

	viper.BindPFlag("source", rootCmd.PersistentFlags().Lookup("source"))
	viper.BindPFlag("destination", rootCmd.PersistentFlags().Lookup("destination"))
	viper.BindPFlag("dialect", rootCmd.PersistentFlags().Lookup("dialect"))
	viper.BindPFlag("connection", rootCmd.PersistentFlags().Lookup("connection"))
}

func er(msg interface{}) {
	fmt.Println("Error:", msg)
	os.Exit(1)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			er(err)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".velocity")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func openDatabase(databaseDialect string, databaseConnection string) (database *gorm.DB, err error) {
	database, err = db.OpenDatabase(databaseDialect, databaseConnection)

	return
}

func fileExists(filename string) bool {
	info, err := fs.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func runRootCommand(source string, destination string) error {
	jww.FEEDBACK.Println("Running ")

	if !fileExists(source) {
		return errors.New("Source file does not exist. Please specify one using the --source flag")
	}

	sourceFile, sourceFileError := fs.Open(source)
	if sourceFileError != nil {
		return sourceFileError
	}
	defer sourceFile.Close()

	var records []Record
	var jsonRecord jsonRecord
	var record Record

	records = make([]Record, 0)

	scanner := bufio.NewScanner(sourceFile)
	for scanner.Scan() {
		text := scanner.Text()
		bytes := []byte(text)

		jsonError := json.Unmarshal(bytes, &jsonRecord)

		if jsonError != nil {
			log.Println(text)
			return jsonError
		} else {
			id, parseError := strconv.ParseInt(jsonRecord.ID, 10, 32)
			if parseError != nil {
				log.Println(text)
				return parseError
			}

			customerId, parseError := strconv.ParseInt(jsonRecord.CustomerID, 10, 32)
			if parseError != nil {
				log.Println(text)
				return parseError
			}

			loadAmount, parseError := strconv.ParseFloat(jsonRecord.LoadAmount[1:], 64)
			if parseError != nil {
				log.Println(text)
				return parseError
			}

			record = Record{
				ID:         uint(id),
				CustomerID: uint((customerId)),
				LoadAmount: loadAmount,
				Time:       jsonRecord.Time,
			}

			records = append(records, record)
		}
	}

	if scannerError := scanner.Err(); scannerError != nil {
		return scannerError
	}

	responses, processError := processRecords(records)

	if processError != nil {
		return processError
	}

	if fileExists(destination) {
		removeError := fs.Remove(destination)

		if removeError != nil {
			return removeError
		}
	}

	destinationFile, destinationFileError := fs.OpenFile(destination,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if destinationFileError != nil {
		return destinationFileError
	}
	defer destinationFile.Close()

	for _, response := range responses {
		if response.ID > 0 {
			jsonResponse := jsonResponse{
				ID:         strconv.FormatInt(int64(response.ID), 10),
				CustomerID: strconv.FormatInt(int64(response.CustomerID), 10),
				Accepted:   response.Accepted,
			}

			responseBytes, responseError := json.Marshal(jsonResponse)

			if responseError != nil {
				return responseError
			}

			responseString := string(responseBytes[:])

			if _, writeError := destinationFile.WriteString(responseString + "\n"); writeError != nil {
				return writeError
			}
		}
	}

	return nil
}

func processRecords(records []Record) ([]Response, error) {
	var responses []Response

	responses = make([]Response, 0, len(records))

	for i := 0; i < len(records); i++ {
		response, error := processRecord(records[i])

		if error != nil {
			return nil, error
		}

		responses = append(responses, response)
	}

	return responses, nil
}

type total struct {
	CustomerID uint
	Total      float64
	Count      uint
}

func processRecord(record Record) (Response, error) {
	response := Response{
		ID:         record.ID,
		CustomerID: record.CustomerID,
		Accepted:   false,
	}

	var dbTransaction db.Transaction

	isoYear, isoWeek := record.Time.ISOWeek()
	year := uint(isoYear)
	week := uint(isoWeek)
	month := uint(record.Time.Month())
	day := uint(record.Time.Day())

	customerTotal := total{}

	// 5000 per day
	database.Table("transactions").Select("customer_id, sum(load_amount) as total, count(load_amount) as count").
		Where("customer_id = ? and year = ? and month = ? and day = ?", record.CustomerID, year, month, day).
		Group("customer_id").First(&customerTotal)

	if record.LoadAmount+customerTotal.Total > 5000 {
		return response, nil
	}

	// 3 times per day
	if customerTotal.Count > 2 {
		return response, nil
	}

	// 20000 per day
	database.Table("transactions").Select("customer_id, sum(load_amount) as total").
		Where("customer_id = ? and year = ? and week = ?", record.CustomerID, year, week).
		Group("customer_id").First(&customerTotal)

	if record.LoadAmount+customerTotal.Total > 20000 {
		return response, nil
	}

	dbTransaction = db.Transaction{
		TransactionID: uint(record.ID),
		CustomerID:    uint(record.CustomerID),
		LoadAmount:    record.LoadAmount,
		Time:          record.Time,
		Year:          year,
		Month:         month,
		Day:           day,
		Week:          week,
	}

	error := database.Save(&dbTransaction).Error

	if error != nil {
		return response, error
	}

	response.Accepted = true

	return response, nil
}
