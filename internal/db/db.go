package db

import (
	"database/sql"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var DB *sql.DB

func InitDB(connStr string) {
	var err error
	DB, err = sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = DB.Ping()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to DB (pgx)")
}

func GetDB() *sql.DB {
	return DB
}
