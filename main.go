package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Endpoints for each source configs are hard coded now. Add the .env parsing and add them in init func if time allows.
var (
	endpointA     = "http://localhost:8080/source-a"
	endpointB     = "http://localhost:8080/source-b"
	endpointC     = "http://localhost:8080/source-c"
	resetEndpoint = "http://localhost:8080/admin/reset"

	// timeout for http call to each endpoint and max try
	timeout              = 5 * time.Second
	maxTry               = 7
	defaultRetryInterval = 500 * time.Millisecond
)

// DTO of "products" field from the source-a json response
type ProductA struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
}

// DTO of "items" field from the source-b json response
type ProductB struct {
	SKU         string `json:"sku"`
	Title       string `json:"title"`
	AmountCents int64  `json:"amount_cents"`
	Department  string `json:"department"`
}

// DTO of "data" field from the source-c json response
type ProductC struct {
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Price       string `json:"price"`
	Type        string `json:"type"`
}

// Normalized product type for the Result
type Product struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Name         string `json:"name"`
	PriceInCents int64  `json:"price_in_cents"`
	Category     string `json:"category"`
}

// raw json response from source-a
type SourceAResponse struct {
	Page       int                       `json:"page"`
	TotalPages int                       `json:"total_pages"`
	Products   GenericProducts[ProductA] `json:"products"`
}

// raw json response from source-a
type SourceBResponse struct {
	NextCursor string                    `json:"next_cursor"`
	Items      GenericProducts[ProductB] `json:"items"`
}

// raw json response from source-a
type SourceCResponse struct {
	NextOffset  int                       `json:"next_offset"`
	MaxPageSize int                       `json:"max_page_size"`
	Data        GenericProducts[ProductC] `json:"data"`
}

// Generic type slice for unmarshalling arbitrarily typed product/item array inside the JSON response.
type GenericProducts[T any] []T

// Custom UnmarshalJSON that gets called by json.Decoder.Decode call.
func (gp *GenericProducts[T]) UnmarshalJSON(data []byte) error {
	var rawList []json.RawMessage
	if err := json.Unmarshal(data, &rawList); err != nil {
		return err
	}

	for _, rawProduct := range rawList {
		var prod T
		if err := json.Unmarshal(rawProduct, &prod); err != nil {
			log.Printf("Error Unmarshalling product %s", rawProduct)
			continue
		}
		*gp = append(*gp, prod)
	}
	return nil
}

type Result struct {
	TotalCount   int `json:"total_count"`
	SourceACount int `json:"source_a_count"`
	SourceBCount int `json:"source_b_count"`
	SourceCCount int `json:"source_c_count"`
	// total fetch duration for receiving, normailizing and queuing(in a channel) in milliseconds
	SourceAFetchDuration int64 `json:"source_a_fetch_duration_ms"`
	SourceBFetchDuration int64 `json:"source_b_fetch_duration_ms"`
	SourceCFetchDuration int64 `json:"source_c_fetch_duration_ms"`
	// list of normalized products from all 3 soruces. Not sorted.
	Products []Product `json:"products"`
}

// return true if retry count exceeds the max retry after increamenting it.
func checkRetryCount(retryCount *int, fetchedCount int, source, fetchedUpto string) bool {
	*retryCount += 1
	if *retryCount > maxTry {
		log.Printf("Max retry %d was exceed from %s endpoint. Abort after fecthing %d products upto %s",
			maxTry, source, fetchedCount, fetchedUpto)
		return true
	}
	return false
}

// Check the "Retry-After" field from the response header.
// If found return it in second, or return the default retry interval.
func tryToGetRetryAfter(res *http.Response, source string) time.Duration {
	ret := defaultRetryInterval
	retryAfter := res.Header.Get("Retry-After")
	if retryAfter != "" {
		sleepInSec, err := strconv.Atoi(retryAfter)
		if err != nil {
			log.Printf("Retry-After field in the header cannot be coverted to an integer: %s", retryAfter)
		} else {
			log.Printf("Retry after set to %s seconds for %s", retryAfter, source)
			ret = time.Duration(sleepInSec) * time.Second
		}
	}
	return ret
}

func fetchSourceA(httpClient *http.Client, wg *sync.WaitGroup, count *int, fetchDuration *int64, prodChan chan<- Product) {
	defer wg.Done()

	retryCount := 0
	currentPage := 1
	totalPage := 1

	start := time.Now()
	for currentPage <= totalPage {
		res, err := httpClient.Get(fmt.Sprintf("%s/products?page=%s", endpointA, strconv.Itoa(currentPage)))
		if err != nil {
			log.Println("Error receiving source-a response")
			if checkRetryCount(&retryCount, *count, "source-a", fmt.Sprintf("page %d", currentPage-1)) {
				break
			}
			time.Sleep(defaultRetryInterval)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode < 200 || res.StatusCode > 300 {
			log.Printf("Http response error from source-a: %v\n", res.StatusCode)
			if checkRetryCount(&retryCount, *count, "source-a", fmt.Sprintf("page %d", currentPage-1)) {
				break
			}
			time.Sleep(defaultRetryInterval)
			continue
		}

		var resA SourceAResponse
		if err != json.NewDecoder(res.Body).Decode(&resA) {
			//skip the entire page assuming there's malformed data in the current page.
			log.Printf("Error decoding JSON response from source-a on page %d. Fetching next page\n", currentPage)
			currentPage += 1
			continue
		}

		for _, v := range resA.Products {
			p := Product{
				ID:           v.ID,
				Source:       "source-a",
				Name:         v.Name,
				PriceInCents: int64(v.Price * 100),
				Category:     v.Category,
			}
			prodChan <- p
		}
		*count += len(resA.Products)
		currentPage += 1
		totalPage = resA.TotalPages
	}

	if currentPage > totalPage {
		log.Println("All pages are fetched from source-a. Finishing fetching source-a.")
	}
	*fetchDuration = time.Since(start).Milliseconds()
}

func fetchSourceB(httpClient *http.Client, wg *sync.WaitGroup, count *int, fetchDuration *int64, prodChan chan<- Product) {
	defer wg.Done()

	retryCount := 0
	cursor := ""
	// a set of fetched cursor to prevent the cursor re-fetching cycle
	fetchedCursor := map[string]struct{}{}

	start := time.Now()
	for {
		// check the cursor re-fetching
		if _, ok := fetchedCursor[cursor]; ok {
			log.Printf("cursor %s has been already fetched. Finishing Fetching source-b.\n", cursor)
			break
		}

		res, err := httpClient.Get(fmt.Sprintf("%s/products?cursor=%s", endpointB, cursor))
		if err != nil {
			log.Println("Error receiving source-b response")
			if checkRetryCount(&retryCount, *count, "source-b", fmt.Sprintf("cursor %s", cursor)) {
				break
			}
			time.Sleep(defaultRetryInterval)
			continue
		}
		defer res.Body.Close()

		if res.StatusCode < 200 || res.StatusCode > 300 {
			log.Printf("Http response error from source-b: %v\n", res.StatusCode)
			if checkRetryCount(&retryCount, *count, "source-b", fmt.Sprintf("cursor %s", cursor)) {
				break
			}
			time.Sleep(tryToGetRetryAfter(res, "source-b"))
			continue
		}

		var resB SourceBResponse
		if err != json.NewDecoder(res.Body).Decode(&resB) {
			log.Println("Error decoding JSON response from source-b")
			log.Printf("Unable to fetch next cursor. Aborting fetching from source-b after fecthing %d products upto cursor %s\n",
				*count, cursor)
			break
		}

		for _, v := range resB.Items {
			p := Product{
				ID:           v.SKU,
				Source:       "source-b",
				Name:         v.Title,
				PriceInCents: v.AmountCents,
				Category:     v.Department,
			}
			prodChan <- p
		}
		*count += len(resB.Items)
		fetchedCursor[cursor] = struct{}{}
		cursor = resB.NextCursor
	}
	*fetchDuration = time.Since(start).Milliseconds()
}

func fetchSourceC(httpClient *http.Client, wg *sync.WaitGroup, count *int, fetchDuration *int64, prodChan chan<- Product) {
	defer wg.Done()

	retryCount := 0
	offset := 0
	limit := 2
	rateLimitMax := 2
	rateLimitRemaining := 2
	rateLimitWindow := 1
	var limitWindowStartedAt time.Time

	start := time.Now()
	for {
		res, err := httpClient.Get(fmt.Sprintf("%s/products?offset=%s&limit=%s", endpointC, strconv.Itoa(offset), strconv.Itoa(limit)))
		if err != nil {
			log.Println("Error receiving source-c response")
			if checkRetryCount(&retryCount, *count, "source-c", fmt.Sprintf("offset %d", offset-limit)) {
				break
			}
			time.Sleep(defaultRetryInterval)
			continue
		}
		defer res.Body.Close()

		// check rate limit window initiation time
		if rateLimitRemaining == rateLimitMax {
			limitWindowStartedAt = time.Now()
		}
		rateLimitRemaining -= 1

		if res.StatusCode < 200 || res.StatusCode > 300 {
			log.Printf("Http response error from source-c: %v\n", res.StatusCode)
			if checkRetryCount(&retryCount, *count, "source-c", fmt.Sprintf("offset %d", offset-limit)) {
				break
			}
			time.Sleep(tryToGetRetryAfter(res, "source-c"))
			continue
		}

		var resC SourceCResponse
		if err != json.NewDecoder(res.Body).Decode(&resC) {
			// skip the entire offset range assuming there's malformed data in the current offset range.
			log.Printf("Error decoding JSON response of offset %d from source-c. Fetching next offset.\n", offset)
			offset += limit
			continue
		}

		pageProdCount := len(resC.Data)
		for _, v := range resC.Data {
			// convert price string to int(in cents)
			priceStr := strings.ReplaceAll(v.Price, ".", "")
			priceInt, err := strconv.Atoi(priceStr)
			if err != nil {
				log.Printf("Invalid price string format for product %v\n", v)
				pageProdCount -= 1
				continue
			}
			p := Product{
				ID:           v.ProductID,
				Source:       "source-c",
				Name:         v.ProductName,
				PriceInCents: int64(priceInt),
				Category:     v.Type,
			}
			prodChan <- p
		}
		*count += pageProdCount

		if resC.NextOffset <= offset {
			log.Println("Offset recycled. Finishing fetching from source-c")
			break
		}
		offset = resC.NextOffset
		limit = resC.MaxPageSize

		// sleep till the end of the current window if the rate limit is reached
		if rateLimitRemaining <= 0 {
			// Update Rate limit max(max try within a window) in case it's updated
			xRateLimitStr := res.Header.Get("X-RateLimit-Limit")
			if xRateLimitStr != "" {
				xRateLimitInt, err := strconv.Atoi(xRateLimitStr)
				if err == nil {
					rateLimitMax = xRateLimitInt
				}
			}
			rateLimitRemaining = rateLimitMax

			// Update Rate Limit Window duration in case it's updated
			xRateLimitWindowStr := res.Header.Get("X-RateLimit-Window")
			if xRateLimitWindowStr != "" {
				xRateLimitWindowInt, err := strconv.Atoi(xRateLimitWindowStr)
				if err == nil {
					rateLimitWindow = xRateLimitWindowInt
				}
			}

			// sleep till the current window ends
			sleepDuration := time.Until(limitWindowStartedAt.Add(time.Duration(rateLimitWindow) * time.Second))
			log.Printf("Sleeping for %d ms till the end of the rate limit window of source-c.\n", sleepDuration.Milliseconds())
			time.Sleep(sleepDuration)
		}
	}

	*fetchDuration = time.Since(start).Milliseconds()
}

func main() {
	httpClient := &http.Client{Timeout: timeout}

	// 3 seperate counters for products from each source.
	// Not shared with main thread concurrently. No need for a lock
	var productACount, productBCount, productCCount int
	// duration for fetch process, including queueing all products, in milliseconds.
	var sourceAFetchDuration, sourceBFetchDuration, sourceCFetchDuration int64

	// 3 producers, 1 consumer(main goroutine) Fan-in pattern using channel
	// Aggregating all products into one single slice(array).
	// Allocating an extra slice(products) will use more memory and receiving data through a single channel might incur contention issue among goroutines,
	// because a "channel" is essentially used like a "locked queue".
	// If output doesn't need to be in a long consequtive memory(a single array with one base pointer), each goroutine can have its own slice that's not shared,
	// and instead of sending products via channel, each goroutine can store them into its own slice, and main goroutine can produce output from those 3 slices.
	// That way, a channel doesn't have to be used and an extra memory allocation for the "products" slice can be avoided.
	// Using the Fan-in pattern with a channel in this example to collect all products into a single array concurrently while fetching.
	products := make([]Product, 0, 16)
	productChan := make(chan Product, 8)
	var wg sync.WaitGroup

	// 3 worker(producer) goroutines fetching from each source
	wg.Add(3)
	go fetchSourceA(httpClient, &wg, &productACount, &sourceAFetchDuration, productChan)
	go fetchSourceB(httpClient, &wg, &productBCount, &sourceBFetchDuration, productChan)
	go fetchSourceC(httpClient, &wg, &productCCount, &sourceCFetchDuration, productChan)

	// sync goroutine closing the shared queue after all sources are fetched
	go func() {
		wg.Wait()
		close(productChan)
	}()

	// consumer routine collecting normailzed products from 3 sources
	for p := range productChan {
		products = append(products, p)
	}
	result := Result{
		TotalCount:           productACount + productBCount + productCCount,
		SourceACount:         productACount,
		SourceBCount:         productBCount,
		SourceCCount:         productCCount,
		SourceAFetchDuration: sourceAFetchDuration,
		SourceBFetchDuration: sourceBFetchDuration,
		SourceCFetchDuration: sourceCFetchDuration,
		Products:             products,
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Result\n%s\n", string(data))
}
