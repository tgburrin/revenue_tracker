package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"revenue_tracker/internal/dal"
	"revenue_tracker/internal/httpconf"

	_ "github.com/lib/pq"
)

func main() {
	var err error

	log.Println("Starting Server")
	if dal.DbPool, err = pgxpool.New(context.Background(), os.Getenv("DATABASE_URL")); err != nil {
		log.Fatalf("Could not connect to database: %v\n", err)
	}
	r := httpconf.SetupRouter()
	if err = r.Run(":8080"); err != nil {
		log.Fatalf("Unable to start server %v\n", err)
	}
}
