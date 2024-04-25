package http

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"net/http"
)

var db = make(map[string]string)

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
	// Authorized group (uses gin.BasicAuth() middleware)
	// Same than:
	// authorized := r.Group("/")
	// authorized.Use(gin.BasicAuth(gin.Credentials{
	//	  "foo":  "bar",
	//	  "manu": "123",
	//}))
	authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
		"foo":  "bar", // user:foo password:bar
		"manu": "123", // user:manu password:123
	}))

	/* example curl for /admin with basicauth header
	   Zm9vOmJhcg== is base64("foo:bar")

		curl -X POST \
	  	http://localhost:8080/admin \
	  	-H 'authorization: Basic Zm9vOmJhcg==' \
	  	-H 'content-type: application/json' \
	  	-d '{"value":"bar"}'
	*/
	authorized.POST("admin", func(c *gin.Context) {
		user := c.MustGet(gin.AuthUserKey).(string)

		// Parse JSON
		var json struct {
			Value string `json:"value" binding:"required"`
		}

		if c.Bind(&json) == nil {
			db[user] = json.Value
			c.JSON(http.StatusOK, gin.H{"status": "ok"})
		}
	})

	return r
}
