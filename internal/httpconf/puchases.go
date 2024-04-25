package httpconf

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"net/http"
	"revenue_tracker/internal/dal"
	"time"
)

type PurchaseEventType struct {
	EventId  uuid.UUID `json:"id" binding:"required"`
	EventDt  time.Time `json:"event_dt" binding:"required"`
	Created  time.Time `json:"created" binding:"required"`
	Paid     time.Time `json:"paid" binding:"required"`
	Customer uuid.UUID `json:"customer" binding:"required"`
	Currency string    `json:"currency" binding:"required"`
	Products []struct {
		ServiceId uuid.UUID `json:"service_id" binding:"required"` // a subscription id or purchase id if there is no term
		ProductId uuid.UUID `json:"product_id" binding:"required"`
		Amount    int32     `json:"amount" binding:"required"` // measured in smallest denomination
		Quantity  int       `json:"quantity" binding:"required"`
		Term      struct {
			Start *time.Time `json:"start"`
			End   *time.Time `json:"end"`
		} `json:"term"`
	} `json:"products" binding:"required"`
}

func HandlePurchaseEvent(c *gin.Context) {
	reqStatus := "error"
	var pe PurchaseEventType
	if err := c.ShouldBindJSON(&pe); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": reqStatus, "error": "Invalid request body: " + err.Error()})
		return
	}

	var txn pgx.Tx
	var err error

	if txn, err = dal.DbPool.Begin(context.Background()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": reqStatus, "error": "Unable to start transaction: " + err.Error()})
		return

	}
	defer func() {
		if err != nil {
			txn.Rollback(context.Background())
		} else {
			txn.Commit(context.Background())
		}
	}()

	for _, product := range pe.Products {
		_, err = txn.Exec(context.Background(),
			"insert into revenue_tracker.revenue_event (event_id, service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)"+
				" values "+
				"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
			pe.EventId,
			product.ServiceId,
			pe.Customer,
			pe.Currency,
			product.Amount*int32(product.Quantity),
			product.Term.Start,
			product.Term.End,
			pe.EventDt,
			pe.Created,
			pe.Paid,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": reqStatus, "error": "Unable to execute transaction: " + err.Error()})
			return
		}
	}

	reqStatus = "success"
	c.JSON(http.StatusOK, gin.H{"status": reqStatus, "event_id": pe.EventId.String()})
	return
}
