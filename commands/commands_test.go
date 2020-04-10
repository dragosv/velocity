package commands

import (
	"bufio"
	guuid "github.com/google/uuid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
	"time"
)

func openTestDatabase() {
	id := guuid.New()
	//
	testDatabase, err := openDatabase("sqlite3", "file:"+id.String()+"?mode=memory")
	if err != nil {
		panic("failed to connect database")
	}

	database = testDatabase
}

func createTestMapFs() {
	fs = afero.NewMemMapFs()
}

func setup() {
	createTestMapFs()
	openTestDatabase()

	source = path.Join("/velocity/source", guuid.New().String())
	destination = path.Join("/velocity/destination", guuid.New().String())
}

func TestRunRootCommand_One_ShouldOutputExpected(t *testing.T) {
	setup()

	afero.WriteFile(fs, source, []byte("{\"id\":\"15887\",\"customer_id\":\"528\",\"load_amount\":\"$3318.47\",\"time\":\"2000-01-01T00:00:00Z\"}\n"), 0644)

	error := runRootCommand(source, destination)

	assert.Nil(t, error)
	assert.True(t, fileExists(destination))

	outputText, error := readAllText(destination)

	assert.Nil(t, error)
	assert.Equal(t, "{\"id\":\"15887\",\"customer_id\":\"528\",\"accepted\":true}", outputText)
}

func TestRunRootCommand_Multiple_ShouldOutputExpected(t *testing.T) {
	setup()

	afero.WriteFile(fs, source, []byte("{\"id\":\"16174\",\"customer_id\":\"766\",\"load_amount\":\"$4112.37\",\"time\":\"2000-01-04T21:04:22Z\"}\n{\"id\":\"5092\",\"customer_id\":\"766\",\"load_amount\":\"$2383.72\",\"time\":\"2000-01-07T01:14:04Z\"}\n{\"id\":\"28835\",\"customer_id\":\"766\",\"load_amount\":\"$5172.48\",\"time\":\"2000-01-10T13:06:08Z\"}\n{\"id\":\"10362\",\"customer_id\":\"766\",\"load_amount\":\"$176.35\",\"time\":\"2000-01-11T23:52:36Z\"}\n{\"id\":\"16934\",\"customer_id\":\"766\",\"load_amount\":\"$201.42\",\"time\":\"2000-01-15T13:47:24Z\"}\n{\"id\":\"31916\",\"customer_id\":\"766\",\"load_amount\":\"$2190.43\",\"time\":\"2000-01-15T16:51:30Z\"}\n{\"id\":\"11526\",\"customer_id\":\"766\",\"load_amount\":\"$126.03\",\"time\":\"2000-01-15T22:59:42Z\"}\n{\"id\":\"10150\",\"customer_id\":\"766\",\"load_amount\":\"$1507.19\",\"time\":\"2000-01-19T18:01:20Z\"}\n{\"id\":\"4824\",\"customer_id\":\"766\",\"load_amount\":\"$2144.67\",\"time\":\"2000-01-21T04:47:48Z\"}\n{\"id\":\"20731\",\"customer_id\":\"766\",\"load_amount\":\"$4169.11\",\"time\":\"2000-01-23T02:49:18Z\"}\n{\"id\":\"25624\",\"customer_id\":\"766\",\"load_amount\":\"$2952.19\",\"time\":\"2000-01-25T06:59:00Z\"}\n{\"id\":\"29071\",\"customer_id\":\"766\",\"load_amount\":\"$4523.37\",\"time\":\"2000-01-26T01:23:36Z\"}\n{\"id\":\"4316\",\"customer_id\":\"766\",\"load_amount\":\"$1559.05\",\"time\":\"2000-01-26T18:46:50Z\"}\n{\"id\":\"5648\",\"customer_id\":\"766\",\"load_amount\":\"$950.10\",\"time\":\"2000-01-30T07:40:16Z\"}\n{\"id\":\"1827\",\"customer_id\":\"766\",\"load_amount\":\"$4522.88\",\"time\":\"2000-01-30T20:58:02Z\"}\n{\"id\":\"31671\",\"customer_id\":\"766\",\"load_amount\":\"$2404.06\",\"time\":\"2000-01-31T01:03:30Z\"}\n{\"id\":\"25458\",\"customer_id\":\"766\",\"load_amount\":\"$4993.25\",\"time\":\"2000-02-02T22:36:26Z\"}\n{\"id\":\"163\",\"customer_id\":\"766\",\"load_amount\":\"$5359.54\",\"time\":\"2000-02-07T03:51:44Z\"}\n"), 0644)

	error := runRootCommand(source, destination)

	assert.Nil(t, error)
	assert.True(t, fileExists(destination))

	outputText, error := readAllText(destination)

	assert.Nil(t, error)
	assert.Equal(t, "{\"id\":\"16174\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"5092\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"28835\",\"customer_id\":\"766\",\"accepted\":false}\n{\"id\":\"10362\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"16934\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"31916\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"11526\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"10150\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"4824\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"20731\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"25624\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"29071\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"4316\",\"customer_id\":\"766\",\"accepted\":false}\n{\"id\":\"5648\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"1827\",\"customer_id\":\"766\",\"accepted\":false}\n{\"id\":\"31671\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"25458\",\"customer_id\":\"766\",\"accepted\":true}\n{\"id\":\"163\",\"customer_id\":\"766\",\"accepted\":false}", outputText)
}

func readAllText(filename string) (text string, err error) {
	sourceFile, sourceFileError := fs.Open(filename)
	if sourceFileError != nil {
		return "", sourceFileError
	}
	defer sourceFile.Close()

	all := ""

	scanner := bufio.NewScanner(sourceFile)
	for scanner.Scan() {
		text := scanner.Text()

		if len(all) > 0 {
			all = all + "\n"
		}

		all = all + text
	}

	return all, nil
}

func TestProcessRecords_LowerThan5000PerDay_ShouldAccept(t *testing.T) {
	setup()

	records := make([]Record, 1)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       time.Now(),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 1, len(responses))
	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)
}

func TestProcessRecords_ThreeTimesPerDay_ShouldAccept(t *testing.T) {
	setup()

	records := make([]Record, 3)

	startDate := time.Date(2020, 11, 9, 3, 51, 48, 324359102, time.UTC)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 2000,
		Time:       startDate,
	}

	records[1] = Record{
		ID:         2,
		CustomerID: 1,
		LoadAmount: 2000,
		Time:       startDate.Add(50),
	}

	records[2] = Record{
		ID:         3,
		CustomerID: 1,
		LoadAmount: 1000,
		Time:       startDate.Add(100),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 3, len(responses))
	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)

	assert.Equal(t, uint(2), responses[1].ID)
	assert.Equal(t, uint(1), responses[1].CustomerID)
	assert.True(t, responses[1].Accepted)

	assert.Equal(t, uint(3), responses[2].ID)
	assert.Equal(t, uint(1), responses[2].CustomerID)
	assert.True(t, responses[2].Accepted)
}

func TestProcessRecords_FourTimesPerDay_ShouldDecline(t *testing.T) {
	setup()

	records := make([]Record, 4)

	startDate := time.Date(2020, 11, 9, 3, 51, 48, 324359102, time.UTC)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 1000,
		Time:       startDate,
	}

	records[1] = Record{
		ID:         2,
		CustomerID: 1,
		LoadAmount: 1000,
		Time:       startDate.Add(50),
	}

	records[2] = Record{
		ID:         3,
		CustomerID: 1,
		LoadAmount: 1000,
		Time:       startDate.Add(100),
	}

	records[3] = Record{
		ID:         4,
		CustomerID: 1,
		LoadAmount: 1000,
		Time:       startDate.Add(150),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 4, len(responses))

	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)

	assert.Equal(t, uint(2), responses[1].ID)
	assert.Equal(t, uint(1), responses[1].CustomerID)
	assert.True(t, responses[1].Accepted)

	assert.Equal(t, uint(3), responses[2].ID)
	assert.Equal(t, uint(1), responses[2].CustomerID)
	assert.True(t, responses[2].Accepted)

	assert.Equal(t, uint(4), responses[3].ID)
	assert.Equal(t, uint(1), responses[3].CustomerID)
	assert.False(t, responses[3].Accepted)
}

func TestProcessRecords_DifferentClientsLessThan5000PerDay_ShouldAccept(t *testing.T) {
	setup()

	records := make([]Record, 2)

	now := time.Now()

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 3000,
		Time:       now,
	}

	records[1] = Record{
		ID:         2,
		CustomerID: 2,
		LoadAmount: 3000,
		Time:       now,
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 2, len(responses))

	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)

	assert.Equal(t, uint(2), responses[1].ID)
	assert.Equal(t, uint(2), responses[1].CustomerID)
	assert.True(t, responses[1].Accepted)
}

func TestProcessRecords_HigherThan5000PerDay_ShouldDecline(t *testing.T) {
	setup()

	records := make([]Record, 1)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 5001,
		Time:       time.Now(),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 1, len(responses))

	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.False(t, responses[0].Accepted)
}

func TestProcessRecords_LowerThan20000PerWeek(t *testing.T) {
	setup()

	records := make([]Record, 4)
	startDate := time.Date(2020, 11, 9, 3, 51, 48, 324359102, time.UTC)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate,
	}

	records[1] = Record{
		ID:         2,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 1),
	}

	records[2] = Record{
		ID:         3,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 2),
	}

	records[3] = Record{
		ID:         4,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 3),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 4, len(responses))

	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)

	assert.Equal(t, uint(2), responses[1].ID)
	assert.Equal(t, uint(1), responses[1].CustomerID)
	assert.True(t, responses[1].Accepted)

	assert.Equal(t, uint(3), responses[2].ID)
	assert.Equal(t, uint(1), responses[2].CustomerID)
	assert.True(t, responses[2].Accepted)

	assert.Equal(t, uint(4), responses[3].ID)
	assert.Equal(t, uint(1), responses[3].CustomerID)
	assert.True(t, responses[3].Accepted)
}

func TestProcessRecords_HigherThan20000PerWeek_ShouldDecline(t *testing.T) {
	setup()

	records := make([]Record, 5)
	startDate := time.Date(2020, 11, 9, 3, 51, 48, 324359102, time.UTC)

	records[0] = Record{
		ID:         1,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate,
	}

	records[1] = Record{
		ID:         2,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 1),
	}

	records[2] = Record{
		ID:         3,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 2),
	}

	records[3] = Record{
		ID:         4,
		CustomerID: 1,
		LoadAmount: 5000,
		Time:       startDate.AddDate(0, 0, 3),
	}

	records[4] = Record{
		ID:         5,
		CustomerID: 1,
		LoadAmount: 1,
		Time:       startDate.AddDate(0, 0, 4),
	}

	responses, error := processRecords(records)

	assert.Nil(t, error)
	assert.Equal(t, 5, len(responses))

	assert.Equal(t, uint(1), responses[0].ID)
	assert.Equal(t, uint(1), responses[0].CustomerID)
	assert.True(t, responses[0].Accepted)

	assert.Equal(t, uint(2), responses[1].ID)
	assert.Equal(t, uint(1), responses[1].CustomerID)
	assert.True(t, responses[1].Accepted)

	assert.Equal(t, uint(3), responses[2].ID)
	assert.Equal(t, uint(1), responses[2].CustomerID)
	assert.True(t, responses[2].Accepted)

	assert.Equal(t, uint(4), responses[3].ID)
	assert.Equal(t, uint(1), responses[3].CustomerID)
	assert.True(t, responses[3].Accepted)

	assert.Equal(t, uint(5), responses[4].ID)
	assert.Equal(t, uint(1), responses[4].CustomerID)
	assert.False(t, responses[4].Accepted)
}
