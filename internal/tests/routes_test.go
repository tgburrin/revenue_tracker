package tests

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"revenue_tracker/internal/httpconf"
	"strings"
	"testing"
)

func TestInitialPurchase(t *testing.T) {
	router := httpconf.SetupRouter()

	var payload string
	if payloadRaw, err := os.ReadFile("testdata/test_purchase.json"); err != nil {
		t.Fatalf("Could not open test data file: %v\n", err)
	} else {
		payload = string(payloadRaw)
	}

	w := httptest.NewRecorder()

	req, _ := http.NewRequest("POST", "/api/v1/purchase/process", strings.NewReader(payload))
	req.Header.Add("Content-Type", "application/json")

	router.ServeHTTP(w, req)

	var resp struct {
		Status  string     `json:"status" required:"true"`
		EventId *uuid.UUID `json:"event_id"`
	}

	assert.Equal(t, 200, w.Code)
	jsonData := json.NewDecoder(w.Body)
	if err := jsonData.Decode(&resp); err != nil {
		t.Fatalf("Could not parse response: %v\n", err)
	}

	assert.Equal(t, "3c1212d0-0281-11ef-8c4d-98fa9b5e176f", resp.EventId.String())
	assert.Equal(t, "success", resp.Status)
}
