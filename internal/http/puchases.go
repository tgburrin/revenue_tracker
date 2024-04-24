package http

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"revenue_tracker/internal/dal"
	"time"
)

type PurchaseEventType struct {
	EventId  dal.InternalId `json:"id" binding:"required"`
	Created  time.Time      `json:"created" binding:"required"`
	Paid     time.Time      `json:"paid" binding:"required"`
	Customer dal.InternalId `json:"customer" binding:"required"`
	Currency string         `json:"currency" binding:"required"`
	Products []struct {
		ServiceId dal.InternalId `json:"service_id" binding:"required"` // a subscription id or purchase id if there is no term
		ProductId dal.InternalId `json:"product_id" binding:"required"`
		Amount    int32          `json:"amount" binding:"required"` // measured in smallest denomination
		Quantity  int            `json:"quantity" binding:"required"`
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
}
