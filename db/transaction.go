package db

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"time"
)

type Transaction struct {
	gorm.Model
	TransactionID uint
	CustomerID    uint
	LoadAmount    float64
	Time          time.Time
	Year          uint
	Month         uint
	Day           uint
	Week          uint
}

func OpenDatabase(databaseDialect string, databaseConnection string) (database *gorm.DB, err error) {
	database, err = gorm.Open(databaseDialect, databaseConnection)
	if err != nil {
		return
	}

	database.LogMode(true)

	// Migrate the schema
	database.AutoMigrate(&Transaction{})

	return
}
