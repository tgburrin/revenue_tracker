package httpconf

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func SetupRouter() *gin.Engine {
	// gin.DisableConsoleColor()
	r := gin.Default()
	cfg := cors.DefaultConfig()
	cfg.AllowHeaders = append(cfg.AllowHeaders, "Access-Control-Allow-Headers", "credentials")
	cfg.AllowAllOrigins = true
	cfg.AllowCredentials = true
	cfg.AllowMethods = append(cfg.AllowMethods, "OPTIONS", "PATCH", "DELETE")
	r.Use(cors.New(cfg))

	api := r.Group("/api/v1")

	invoiceRoutes := api.Group("/purchase")
	invoiceRoutes.POST("/process", HandlePurchaseEvent)

	revenueRoutes := api.Group("/revenue")
	revenueRoutes.POST("/by_date", HandleRevenueByDateRequest)
	revenueRoutes.POST("/by_month", HandleRevenueByMonthRequest)
	return r
}
