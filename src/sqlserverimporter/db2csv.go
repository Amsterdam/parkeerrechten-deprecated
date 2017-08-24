package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"encoding/csv"
	"errors"
	"reflect"
	"strconv"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	debug = flag.Bool("debug", false, "enable debugging")
	password = flag.String("password", "", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server = flag.String("server", "", "the database server")
	user = flag.String("user", "", "the database user")
	database = flag.String("database", "", "the database schema")
	query = flag.String("query", "", "the query to run")
	dbtype = flag.String("dbtype", "mssql", "databasetype: mssql")
	output = flag.String("output", "/tmp/export/output.csv", "output file")
)

func main() {
	flag.Parse()

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
		fmt.Printf(" database:%s\n", *database)
		fmt.Printf(" query:%s\n", *query)
		fmt.Printf(" dbtype:%s\n", *dbtype)
		fmt.Printf(" output:%s\n", *output)
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;query=%s;", *server, *user, *password, *port, *database, *query)
	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}
	conn, err := sql.Open(*dbtype, connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	rows, err := conn.Query(*query)
	if err != nil {
		log.Fatal("Prepare failed:", err.Error())
	}
	defer rows.Close()

	file, err := os.Create(*output)
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	columns, _ := rows.Columns()
	count := len(columns)
	valuePtrs := make([]interface{}, count)
	values := make([]interface{}, count)
	for i, _ := range columns {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(valuePtrs...)
		to_string_array(values)
		err := writer.Write(to_string_array(values))
		checkError("Cannot write to file", err)
	}

	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}

	fmt.Printf("bye\n")
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func checkThisError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func to_string_array(input []interface{}) []string {

	output := make([]string, len(input))

	for i, v := range input {
		str, err := to_string(v)
		checkThisError(err)
		output[i] = str
	}
	return output
}

func to_string(v interface{}) (str string, err error) {
	switch t := v.(type) {
	case []uint8:
		str = string(v.([]uint8))
	case string:
		str = v.(string)
	case int64:
		str = strconv.FormatInt(v.(int64), 10)
	case nil:
		str = ""
	default:
		var msg string
		if t != nil {
			msg = "Unknown datatype: " + reflect.TypeOf(v).Name()
		} else {
			msg = "Datatype: Nil"
		}
		err = errors.New(msg)
	}
	return
}