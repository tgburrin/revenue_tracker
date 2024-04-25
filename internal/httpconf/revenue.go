package httpconf

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

type Date time.Time

func (t *Date) UnmarshalJSON(b []byte) (err error) {
	//date, err := time.Parse(`"2006-01-02T15:04:05.000-0700"`, string(b))
	if date, err := time.Parse(`"2006-01-02"`, string(b)); err != nil {
		return err
	} else {
		*t = Date(date)
	}
	return nil
}

func (t *Date) String() string {
	return time.Time(*t).Format("2006-01-02")
}

func HandleRevenueByDateRequest(c *gin.Context) {
	reqStatus := "error"
	var reqBody struct {
		RevenueDate  Date       `json:"revenue_date" binding:"required"`
		POVTimestamp *time.Time `json:"pov_timestamp"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": reqStatus, "error": "Invalid request body: " + err.Error()})
		return
	}
	if reqBody.POVTimestamp != nil {
		fmt.Printf("%s -> %s\n", reqBody.RevenueDate.String(), reqBody.POVTimestamp.String())
	} else {
		fmt.Printf("%s\n", reqBody.RevenueDate.String())
	}
}

func HandleRevenueByMonthRequest(c *gin.Context) {
	reqStatus := "error"
	var reqBody struct {
		RevenueDate  Date       `json:"revenue_date" binding:"required"`
		POVTimestamp *time.Time `json:"pov_timestamp"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": reqStatus, "error": "Invalid request body: " + err.Error()})
		return
	}
	if reqBody.POVTimestamp != nil {
		fmt.Printf("%s -> %s\n", reqBody.RevenueDate.String(), reqBody.POVTimestamp.String())
	} else {
		fmt.Printf("%s\n", reqBody.RevenueDate.String())
	}
}
