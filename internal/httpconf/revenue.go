package httpconf

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"revenue_tracker/internal/dal"
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

	type revenueSummaryType struct {
		CurrencyCode     string  `json:"currency_code"`
		ServiceSubs      int     `json:"service_subs"`
		RecognizedAmount float64 `json:"recognized_amount"`
	}

	var responseBody []*revenueSummaryType

	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": reqStatus, "error": "Invalid request body: " + err.Error()})
		return
	}

	query := `select
	r.currency_code,
	count(distinct r.service_id) as service_subs,
	sum(rev.recognized_amount) as recognized_amount
from
	revenue_tracker.revenue_event r
	left join revenue_tracker.revenue_event rf on
			rf.service_id = r.service_id
		and rf.valid_from_ts = r.valid_to_ts
	cross join revenue_tracker.calculate_event_revenue(
		event => r,
		next_event_start_dt => lower(rf.revenue_ts),
		revenue_query_range => ('['||$1||','||$1||']')::daterange
        ) as rev
group by 1
	`
	if rows, err := dal.DbPool.Query(context.Background(), query, reqBody.RevenueDate.String()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": reqStatus, "error": "Unable to query revenue data: " + err.Error()})
		return
	} else {
		for rows.Next() {
			var r revenueSummaryType
			if err := rows.Scan(&r.CurrencyCode, &r.ServiceSubs, &r.RecognizedAmount); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"status": reqStatus, "error": "Unable to parse revenue data"})
				return
			} else {
				responseBody = append(responseBody, &r)
			}
		}

	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "revenue_summary": responseBody})
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
