package main

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var client *http.Client

var apiKey = os.Getenv("hs-api-key")

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

func ConstructUrl(baseUrl string, propertiesWithHistory string, properties string, offset int) string {
	base, baseErr := url.Parse(baseUrl)
	if baseErr != nil {
		return baseErr.Error()
	}
	// base.Path += ""
	params := url.Values{}
	params.Add("hapikey", apiKey)
	params.Add("limit", "250")
	params.Add("propertiesWithHistory", propertiesWithHistory)
	params.Add("properties", properties)
	if offset > 0 {
		params.Add("offset", strconv.Itoa(offset))
	}
	base.RawQuery = params.Encode()
	return base.String()
}

func GetDealHistory() []byte {
	var dealHistory DealHistory
	hasMore := true
	count := 0
	var buf bytes.Buffer
	var offset int
	for hasMore {
		constructUrl := ConstructUrl("https://api.hubapi.com/deals/v1/deal/paged", "dealstage", "dealId", offset)
		err := GetJson(constructUrl, &dealHistory)
		fmt.Println(constructUrl)
		if err != nil {
			fmt.Printf("error getting json: %s\n", err.Error())
		} else {
			fmt.Printf("successfully downloaded json - has more?: %+v\n", dealHistory.HasMore)

			for _, loc := range dealHistory.Deals {
				encoder := json.NewEncoder(&buf)
				_ = encoder.Encode(loc)
			}

			count += 1
			if count >= 150 {
				hasMore = false
				break
			}
			hasMore = dealHistory.HasMore
			if !hasMore {
				break
			}
			offset = dealHistory.Offset
		}
	}
	return buf.Bytes()
}

func GetJson(url string, target interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)
	return json.NewDecoder(resp.Body).Decode(target)
}
// streamFileUpload uploads to an object via a stream without reading to memory.
func streamFileUpload(bucket string, object string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer func(client *storage.Client) {
		err := client.Close()
		if err != nil {
		}
	}(client)

	buf := bytes.NewBuffer(GetDealHistory())
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	wc.ChunkSize = 0 // note retries are not supported for chunk size 0.
	if _, err = io.Copy(wc, buf); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	// Data can continue to be added to the file until the writer is closed.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	log.WithFields(
		log.Fields{
			"foo": "foo",
			"bar": "bar",
		},
	).Info("%v uploaded to %v.\n\n", object, bucket)

	return nil
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	start := time.Now()
	client = &http.Client{Timeout: 10 * time.Second}
	err := streamFileUpload("", "output.json")
	if err != nil {
		return
	}
	elapsed := time.Since(start)
	log.Printf("Process took %s", elapsed)
}
