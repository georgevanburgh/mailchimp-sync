package main

import (
	"database/sql"
	"fmt"
	"os"

	mailchimpsync "github.com/FireEater64/MUMS-MailChimp-Sync/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mattbaird/gochimp"
	"github.com/urfave/cli"
)

func validateArgs(c *cli.Context) {
	if !c.IsSet("mailchimp-api-key") {
		panic("Require mailchimp api key")
	}
	if !c.IsSet("mysql-connection-string") {
		panic("Require mysql connection string")
	}
	if !c.IsSet("mailchimp-list-name") {
		panic("Require mailchimp list name")
	}
}

func getEntriesToSync(connectionString string, query string) *[]mailchimpsync.Entry {
	var toReturn []mailchimpsync.Entry

	// Connect to the database
	db, connectError := sql.Open("mysql", connectionString)
	defer db.Close()

	if connectError != nil {
		panic("Could not connect to MySQL database")
	}

	rows, queryErr := db.Query(query)
	if queryErr != nil {
		panic("Error whilst executing query: " + queryErr.Error())
	}
	defer rows.Close()

	for rows.Next() {
		toAdd := mailchimpsync.Entry{}

		// Either 1 row (email address)
		var readError error
		cols, rowError := rows.Columns()
		if rowError != nil {
			panic(rowError)
		}

		if len(cols) == 1 {
			readError = rows.Scan(&toAdd.EmailAddress)
		} else if len(cols) == 3 {
			readError = rows.Scan(&toAdd.FirstName, &toAdd.LastName, &toAdd.EmailAddress)
		} else {
			panic("Invalid number of columns returned")
		}
		if readError != nil {
			panic(readError)
		}

		toReturn = append(toReturn, toAdd)
	}

	retrieveErr := rows.Err()
	if retrieveErr != nil {
		panic("Error retrieving rows from database" + retrieveErr.Error())
	}

	return &toReturn
}

func syncMailChimp(c *cli.Context) {
	validateArgs(c)

	mysqlConnectionString := c.String("mysql-connection-string")
	mysqlQuery := c.String("mysql-query")
	mailChimpApiKey := c.String("mailchimp-api-key")
	mailChimpList := c.String("mailchimp-list-name")

	subscribers := getEntriesToSync(mysqlConnectionString, mysqlQuery)
	fmt.Printf("Synchronising %d entries\n", len(*subscribers))

	// Connect to mailchimp
	chimpApi := gochimp.NewChimp(mailChimpApiKey, true)

	batchUpdateRequest := gochimp.BatchSubscribe{
		ApiKey:         mailChimpApiKey,
		ListId:         mailChimpList,
		UpdateExisting: true,
		DoubleOptin:    false,
	}

	for _, subscriber := range *subscribers {
		email := gochimp.Email{Email: subscriber.EmailAddress}
		toAdd := gochimp.ListsMember{
			Email: email,
		}

		// Add first/last name if they exist
		if subscriber.FirstName != "" && subscriber.LastName != "" {
			toAdd.MergeVars = make(map[string]interface{})
			toAdd.MergeVars["FNAME"] = subscriber.FirstName
			toAdd.MergeVars["LNAME"] = subscriber.LastName
		}

		batchUpdateRequest.Batch = append(batchUpdateRequest.Batch, toAdd)
	}

	response, requestErr := chimpApi.BatchSubscribe(batchUpdateRequest)
	if requestErr != nil {
		panic(requestErr)
	}

	fmt.Printf("Added: %d, Updated: %d, Error: %d\n", response.AddCount, response.UpdateCount, response.ErrorCount)

	for _, erroredAddress := range response.Error {
		fmt.Printf("%s\n", erroredAddress.Error)
	}
}

func main() {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "m, mailchimp-api-key",
			Usage:  "The API key used to connect to MailChimp",
			EnvVar: "MAILCHIMP_API_KEY",
		},
		cli.StringFlag{
			Name:   "c, mysql-connection-string",
			Usage:  "The connection-string used to connect to MySQL",
			EnvVar: "MYSQL_USERNAME",
		},
		cli.StringFlag{
			Name:  "q, mysql-query",
			Usage: "The query used to retrieve email address, and optionally first and last names. Columns should be returned in 'first, last, email' order.",
			Value: "SELECT firstname, lastname, email FROM members",
		},
		cli.StringFlag{
			Name:  "l, mailchimp-list-name",
			Usage: "The mailchimp list to sync users with",
		},
	}

	app.Name = "mailchimp-sync"
	app.Usage = "sync mysql database with MailChimp"
	app.Version = "0.0.1"

	app.Action = syncMailChimp

	app.Run(os.Args)
}
