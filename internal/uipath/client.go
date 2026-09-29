// Package uipath implements read-only OData collection and OAuth client credentials.
package uipath

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
)

type HTTPError struct{ Status int }

func (e *HTTPError) Error() string { return fmt.Sprintf("API HTTP %d", e.Status) }

var ErrCapped = errors.New("source result reached configured limit; coverage incomplete")

type Folder struct {
	ID   int64  `json:"Id"`
	Name string `json:"DisplayName"`
}
type Job struct {
	ID           int64 `json:"Id"`
	Key          string
	State        string
	ReleaseName  string
	CreationTime time.Time
	StartTime    *time.Time
	EndTime      *time.Time
}
type RobotLog struct {
	ID                          int64 `json:"Id"`
	JobKey                      string
	TimeStamp                   time.Time
	Level, Message, ProcessName string
}
type QueueDefinition struct {
	ID   int64 `json:"Id"`
	Name string
}
type QueueItem struct {
	ID                                                 int64 `json:"Id"`
	QueueDefinitionID                                  int64 `json:"QueueDefinitionId"`
	Status                                             string
	CreationTime                                       time.Time
	StartProcessing, EndProcessing, DeferDate, DueDate *time.Time
	RetryNumber                                        int
	ProcessingExceptionType                            string
}
type Client struct {
	cfg     config.Config
	HTTP    *http.Client
	mu      sync.Mutex
	token   string
	expires time.Time
}

func New(c config.Config) (*Client, error) {
	hc, e := config.HTTPClient(c.Timeout, c.CAFile, c.Insecure, "", "")
	if e != nil {
		return nil, e
	}
	if c.DialAddress != "" {
		u, _ := url.Parse(c.URL)
		host := u.Hostname()
		tr := hc.Transport.(*http.Transport)
		old := tr.DialContext
		tr.Proxy = nil
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			h, _, e := net.SplitHostPort(address)
			if e == nil && h == host {
				address = c.DialAddress
			}
			return old(ctx, network, address)
		}
	}
	return &Client{cfg: c, HTTP: hc}, nil
}
func (c *Client) auth(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.expires) {
		return c.token, nil
	}
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {c.cfg.ClientID}, "client_secret": {c.cfg.Secret}, "scope": {c.cfg.Scopes}}
	req, e := http.NewRequestWithContext(ctx, "POST", c.cfg.TokenURL, strings.NewReader(form.Encode()))
	if e != nil {
		return "", errors.New("invalid token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, e := c.HTTP.Do(req)
	if e != nil {
		return "", errors.New("OAuth transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("OAuth HTTP %d", resp.StatusCode)
	}
	var t struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&t); e != nil || t.AccessToken == "" {
		return "", errors.New("invalid OAuth response")
	}
	if t.ExpiresIn < 1 {
		t.ExpiresIn = 300
	}
	c.token = t.AccessToken
	c.expires = time.Now().Add(time.Duration(t.ExpiresIn) * time.Second * 9 / 10)
	return c.token, nil
}
func (c *Client) page(ctx context.Context, path string, folder int64, q url.Values) ([]json.RawMessage, error) {
	for attempt := 0; attempt < 3; attempt++ {
		token, e := c.auth(ctx)
		if e != nil {
			return nil, e
		}
		req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(c.cfg.URL, "/")+path+"?"+q.Encode(), nil)
		if e != nil {
			return nil, errors.New("invalid API request")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		if folder > 0 {
			req.Header.Set("X-UIPATH-OrganizationUnitId", strconv.FormatInt(folder, 10))
		}
		resp, e := c.HTTP.Do(req)
		if e != nil {
			return nil, errors.New("API transport failed")
		}
		if resp.StatusCode == 401 {
			resp.Body.Close()
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
			continue
		}
		if resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504 {
			wait := time.Duration(attempt+1) * time.Second
			if sec, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && sec > 0 {
				wait = time.Duration(sec) * time.Second
			}
			if t, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil && time.Until(t) > 0 {
				wait = time.Until(t)
			}
			status := resp.StatusCode
			resp.Body.Close()
			if wait > 30*time.Second || attempt == 2 {
				return nil, fmt.Errorf("API HTTP %d; retry deferred", status)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, &HTTPError{Status: resp.StatusCode}
		}
		var result struct {
			Value []json.RawMessage `json:"value"`
			Next  string            `json:"@odata.nextLink"`
		}
		e = json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result)
		resp.Body.Close()
		if e != nil || result.Value == nil {
			return nil, errors.New("invalid OData response")
		}
		if result.Next != "" {
			return nil, errors.New("server-driven pagination requires an explicit continuation strategy; coverage incomplete")
		}
		return result.Value, nil
	}
	return nil, errors.New("API authentication retry exhausted")
}

// List uses bounded offset pagination. Reaching the cap is deliberately an error,
// even when the true result count might exactly equal the cap.
func (c *Client) List(ctx context.Context, path string, folder int64, q url.Values) ([]json.RawMessage, error) {
	if q == nil {
		q = url.Values{}
	}
	var all []json.RawMessage
	for len(all) < c.cfg.MaxRecords {
		size := min(c.cfg.PageSize, c.cfg.MaxRecords-len(all))
		q.Set("$top", strconv.Itoa(size))
		q.Set("$skip", strconv.Itoa(len(all)))
		rows, e := c.page(ctx, path, folder, q)
		if e != nil {
			return nil, e
		}
		all = append(all, rows...)
		if len(rows) < size {
			return all, nil
		}
	}
	return nil, ErrCapped
}
func decode[T any](rows []json.RawMessage, e error) ([]T, error) {
	if e != nil {
		return nil, e
	}
	out := make([]T, 0, len(rows))
	for _, r := range rows {
		var v T
		if e := json.Unmarshal(r, &v); e != nil {
			return nil, errors.New("invalid source record")
		}
		out = append(out, v)
	}
	return out, nil
}
func (c *Client) Folders(ctx context.Context) ([]Folder, error) {
	r, e := c.List(ctx, "/odata/Folders", 0, url.Values{"$select": {"Id,DisplayName"}, "$orderby": {"Id asc"}})
	return decode[Folder](r, e)
}
func (c *Client) Jobs(ctx context.Context, id int64, since time.Time) ([]Job, error) {
	filter := "(CreationTime ge " + since.UTC().Format(time.RFC3339Nano) + " or EndTime ge " + since.UTC().Format(time.RFC3339Nano) + " or State eq 'Pending' or State eq 'Running' or State eq 'Stopping' or State eq 'Terminating' or State eq 'Suspended' or State eq 'Resumed')"
	r, e := c.List(ctx, "/odata/Jobs", id, url.Values{"$select": {"Id,Key,State,ReleaseName,CreationTime,StartTime,EndTime"}, "$filter": {filter}, "$orderby": {"Id asc"}})
	return decode[Job](r, e)
}
func (c *Client) Logs(ctx context.Context, id int64, since time.Time) ([]RobotLog, error) {
	fields := "Id,JobKey,TimeStamp,Level,ProcessName"
	if c.cfg.IncludeMessages {
		fields += ",Message"
	}
	r, e := c.List(ctx, "/odata/RobotLogs", id, url.Values{"$select": {fields}, "$filter": {"TimeStamp gt " + since.UTC().Format(time.RFC3339Nano)}, "$orderby": {"TimeStamp asc"}})
	return decode[RobotLog](r, e)
}
func (c *Client) QueueDefinitions(ctx context.Context, id int64) ([]QueueDefinition, error) {
	r, e := c.List(ctx, "/odata/QueueDefinitions", id, url.Values{"$select": {"Id,Name"}, "$orderby": {"Id asc"}})
	return decode[QueueDefinition](r, e)
}
func (c *Client) QueueItems(ctx context.Context, id int64, since time.Time) ([]QueueItem, error) {
	r, e := c.List(ctx, "/odata/QueueItems", id, url.Values{"$select": {"Id,QueueDefinitionId,Status,CreationTime,StartProcessing,EndProcessing,RetryNumber,ProcessingExceptionType,DeferDate,DueDate"}, "$filter": {"Status eq 'New' or Status eq 'InProgress' or EndProcessing ge " + since.UTC().Format(time.RFC3339Nano)}, "$orderby": {"Id asc"}})
	return decode[QueueItem](r, e)
}
