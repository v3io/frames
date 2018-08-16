package v3ioframes

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// Message is returned messge type
type Message struct {
}

// Client is v3io streaming client
type Client struct {
	URL    string
	apiKey string
}

// NewClient returns a new client
func NewClient(url string, apiKey string) *Client {
	return &Client{
		URL:    url,
		apiKey: apiKey,
	}
}

// Query runs a query on the client
func (c *Client) Query(query string) (chan *Message, error) {
	queryObj := map[string]interface{}{
		"query":   query,
		"limit":   100,
		"columns": []string{"first", "last"},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(queryObj); err != nil {
		return nil, errors.Wrap(err, "can't encode query")
	}

	req, err := http.NewRequest("POST", c.URL, &buf)
	if err != nil {
		return nil, errors.Wrap(err, "can't create HTTP request")
	}
	req.Header.Set("Autohrization", c.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "can't call API")
	}

	ch := make(chan *Message) // TODO: Buffered channel?

	go func() {
		defer resp.Body.Close()
		dec := msgpack.NewDecoder(resp.Body)
		for {
			msg := &Message{}
			if err := dec.Decode(msg); err != nil {
				// TODO: log
				return
			}
			ch <- msg
		}
	}()

	return ch, nil
}
