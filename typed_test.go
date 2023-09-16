package typed

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

type client struct {
	ClientID   int
	ClientName string
	Address    string
	Industry   string
}

func (c *client) TableName() string {
	return "client"
}

func (c *client) Columns() []Pair[string, interface{}] {
	return []Pair[string, interface{}]{
		{"client_id", c.ClientID},
		{"client_name", c.ClientName},
		{"address", c.Address},
		{"industry", c.Industry},
	}
}

func (c *client) PrimaryKeyColumn() string {
	return "client_id"
}

func (c *client) PrimaryKeyValue() interface{} {
	return c.ClientID
}

func (c *client) ScanRow(rows *sql.Rows) error {
	return rows.Scan(&c.ClientID, &c.ClientName, &c.Address, &c.Industry)
}

func (c *client) New() *client {
	return &client{}
}

func TestCreate(t *testing.T) {
	fmt.Println("Begin test")
	conn, err := sql.Open("sqlite3", ":memory:")

	apple := client{
		ClientID:   420,
		ClientName: "Apple Inc.",
		Address:    "123 Main St.",
		Industry:   "Consumer",
	}

	db := &DB{conn}

	err = NewTable(db, &client{})
	if err != nil {
		return
	}

	err = Create(db, &apple)
	if err != nil {
		return
	}

	clients, err := Read(db, &client{})

	if err != nil {
		log.Fatal(err)
	}

	const NUMBER_OF_CLIENTS = 1

	if len(clients) != NUMBER_OF_CLIENTS {
		t.Errorf("\nExpected to receive %d clients, but actually recieved %d\n", NUMBER_OF_CLIENTS, len(clients))
	}

	first := clients[0]
	const CLIENT_ID = 420
	const CLIENT_NAME = "Apple Inc."
	const ADDRESS = "123 Main St."
	const INDUSTRY = "Consumer"

	if first.ClientID != CLIENT_ID || first.ClientName != CLIENT_NAME || first.Address != ADDRESS || first.Industry != INDUSTRY {
		t.Errorf("\nExpected:\n{  client_id: %d\n   client_name: %s\n   address: 123 %s\n   industry: %s  }\n"+
			"Actual:\n{  client_id: %d\n   client_name: %s\n   address: %s\n   industry: %s  }",
			CLIENT_ID, CLIENT_NAME, ADDRESS, INDUSTRY, first.ClientID, first.ClientName, first.Address, first.Industry)
	}
}
