package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"os"
)

var client *http.Client

var api_key = os.Getenv("hs-api-key")

type DealHistory struct {
	Deals []struct {
		PortalID     int         `json:"portalId"`
		DealID       int64       `json:"dealId"`
		IsDeleted    bool        `json:"isDeleted"`
		Associations interface{} `json:"associations"`
		Properties   struct {
			Dealstage struct {
				Value           string `json:"value"`
				Timestamp       int64  `json:"timestamp"`
				Source          string `json:"source"`
				SourceID        string `json:"sourceId"`
				UpdatedByUserID int    `json:"updatedByUserId"`
				Versions        []struct {
					Name            string        `json:"name"`
					Value           string        `json:"value"`
					Timestamp       int64         `json:"timestamp"`
					SourceID        string        `json:"sourceId"`
					Source          string        `json:"source"`
					SourceVid       []interface{} `json:"sourceVid"`
					RequestID       string        `json:"requestId"`
					UpdatedByUserID int           `json:"updatedByUserId"`
				} `json:"versions"`
			} `json:"dealstage"`
		} `json:"properties"`
		StateChanges []interface{} `json:"stateChanges"`
	} `json:"deals"`
	HasMore bool `json:"hasMore"`
	Offset  int  `json:"offset"`
}

func ConstructUrl(base_url string, propertiesWithHistory string, properties string, offset int) string {
	base, base_err := url.Parse(base_url)
	if base_err != nil {
		return base_err.Error()
	}
	// base.Path += ""
	params := url.Values{}
	params.Add("hapikey", api_key)
	params.Add("limit", "250")
	params.Add("propertiesWithHistory", propertiesWithHistory)
	params.Add("properties", properties)
	if offset > 0 {
		params.Add("offset", strconv.Itoa(offset))
	}
	base.RawQuery = params.Encode()
	return base.String()
}

func GetDealHistory() {
	var dealHistory DealHistory
	has_more := true
	count := 0
	var buf bytes.Buffer
	var offset int
	for has_more {
		url := ConstructUrl("https://api.hubapi.com/deals/v1/deal/paged", "dealstage", "dealId", offset)
		err := GetJson(url, &dealHistory)
		fmt.Println(url)
		if err != nil {
			fmt.Printf("error getting json: %s\n", err.Error())
			return
		} else {
			fmt.Printf("successfully downloaded json - has more?: %+v\n", dealHistory.HasMore)

			for _, loc := range dealHistory.Deals {
				encoder := json.NewEncoder(&buf)
				_ = encoder.Encode(loc)
			}

			count += 1
			if count >= 85 {
				has_more = false
				break
			}
			has_more = dealHistory.HasMore
			if !has_more {
				break
			}
			offset = dealHistory.Offset
		}
	}
	_ = ioutil.WriteFile("big_encode.json", buf.Bytes(), 0644)
}

func GetJson(url string, target interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(target)
}

func main() {
	client = &http.Client{Timeout: 10 * time.Second}
	GetDealHistory()
}
