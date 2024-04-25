package tests

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"revenue_tracker/internal/dal"
	"testing"
)

var dbUri = "postgresql://localhost/revenue_tracker_test"

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func setup() {
	var err error
	var iPool *pgxpool.Pool

	if iPool, err = pgxpool.New(context.Background(), "postgresql://localhost/"); err != nil {
		log.Fatalf("Could not connect to database: %v\n", err)
	}
	defer iPool.Close()

	if _, err = iPool.Exec(context.Background(), "create database revenue_tracker_test"); err != nil {
		log.Fatalf("Could not create database: %v\n", err)
	}

	if dal.DbPool, err = pgxpool.New(context.Background(), dbUri); err != nil {
		log.Fatalf("Could not connect to database: %v\n", err)
	}

	var ddl string
	if ddlRaw, err := os.ReadFile("../sql/amort_funcs.sql"); err != nil {
		log.Fatalf("Could not open ddl: %v\n", err)
	} else {
		ddl = string(ddlRaw)
	}

	if _, err := dal.DbPool.Exec(context.Background(), ddl); err != nil {
		log.Fatalf("Could not apply ddl: %v\n", err)
	}
}

func shutdown() {
	var err error
	var iPool *pgxpool.Pool

	dal.DbPool.Close()

	if iPool, err = pgxpool.New(context.Background(), "postgresql://localhost/"); err != nil {
		log.Fatalf("Could not connect to database: %v\n", err)
	}
	defer iPool.Close()

	if _, err = iPool.Exec(context.Background(), "drop database revenue_tracker_test"); err != nil {
		log.Fatalf("Could not drop database: %v\n", err)
	}
}
