package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Client calls the API server over HTTP.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: http.DefaultClient}
}

// Create posts obj to its collection. An empty namespace becomes "default".
// Decode the 201 body back into obj so UID and resourceVersion are filled in.
// 409 is registry.ErrConflict. 400 is an error.
func (c *Client) Create(ctx context.Context, obj api.Object) error {
	meta := obj.GetObjectMeta()
	if obj.Kind() != "Node" && meta.Namespace == "" {
		meta.Namespace = "default"
	}
	path, err := collectionPath(obj.Kind(), meta.Namespace)
	if err != nil {
		return err
	}
	return c.write(ctx, http.MethodPost, path, obj, http.StatusCreated)
}

func (c *Client) Get(ctx context.Context, kind, namespace, name string) (api.Object, error) {
	path, err := objectPath(kind, namespace, name)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, registry.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: get failed: %s", resp.Status)
	}
	obj, err := newObject(kind)
	if err != nil {
		return nil, err
	}
	if err := json.NewDecoder(resp.Body).Decode(obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (c *Client) Update(ctx context.Context, obj api.Object) error {
	meta := obj.GetObjectMeta()
	path, err := objectPath(obj.Kind(), meta.Namespace, meta.Name)
	if err != nil {
		return err
	}
	return c.write(ctx, http.MethodPut, path, obj, http.StatusOK)
}

func (c *Client) UpdateStatus(ctx context.Context, obj api.Object) error {
	meta := obj.GetObjectMeta()
	path, err := objectPath(obj.Kind(), meta.Namespace, meta.Name)
	if err != nil {
		return err
	}
	return c.write(ctx, http.MethodPut, path+"/status", obj, http.StatusOK)
}

func (c *Client) Delete(ctx context.Context, kind, namespace, name string) error {
	path, err := objectPath(kind, namespace, name)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return registry.ErrNotFound
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("client: delete failed: %s", resp.Status)
	}
	return nil
}

func (c *Client) List(ctx context.Context, kind, namespace string) ([]api.Object, uint64, error) {
	path, err := collectionPath(kind, namespace)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("client: list failed: %s", resp.Status)
	}
	return decodeList(kind, resp)
}

func (c *Client) Watch(ctx context.Context, kind, namespace string, rev uint64) (<-chan registry.Event, error) {
	path, err := collectionPath(kind, namespace)
	if err != nil {
		return nil, err
	}
	path += "?watch=1&resourceVersion=" + strconv.FormatUint(rev, 10)
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("client: watch failed: %s", resp.Status)
	}
	ch := make(chan registry.Event)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				ev, decErr := decodeEvent(kind, line)
				if decErr == nil {
					select {
					case ch <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return ch, nil
}

func (c *Client) write(ctx context.Context, method, path string, obj api.Object, okStatus int) error {
	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return registry.ErrConflict
	}
	if resp.StatusCode != okStatus {
		return fmt.Errorf("client: %s failed: %s", method, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(obj)
}

func (c *Client) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	var reqBody *bytes.Reader
	reqBody = reader
	var req *http.Request
	var err error
	if reqBody == nil {
		req, err = http.NewRequestWithContext(ctx, method, c.BaseURL+path, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, c.BaseURL+path, reqBody)
	}
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.HTTP.Do(req)
}

func collectionPath(kind, namespace string) (string, error) {
	return objectPath(kind, namespace, "")
}

func objectPath(kind, namespace, name string) (string, error) {
	var path string
	switch kind {
	case "Pod":
		if namespace == "" {
			namespace = "default"
		}
		path = "/api/v1/namespaces/" + namespace + "/pods"
	case "ReplicaSet":
		if namespace == "" {
			namespace = "default"
		}
		path = "/api/v1/namespaces/" + namespace + "/replicasets"
	case "Node":
		path = "/api/v1/nodes"
	default:
		return "", fmt.Errorf("client: unknown kind %s", kind)
	}
	if name != "" {
		path += "/" + name
	}
	return path, nil
}

func newObject(kind string) (api.Object, error) {
	switch kind {
	case "Pod":
		return &api.Pod{}, nil
	case "Node":
		return &api.Node{}, nil
	case "ReplicaSet":
		return &api.ReplicaSet{}, nil
	default:
		return nil, fmt.Errorf("client: unknown kind %s", kind)
	}
}

func decodeList(kind string, resp *http.Response) ([]api.Object, uint64, error) {
	switch kind {
	case "Pod":
		var page struct {
			ResourceVersion uint64    `json:"resourceVersion"`
			Items           []api.Pod `json:"items"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, 0, err
		}
		items := make([]api.Object, len(page.Items))
		for i := range page.Items {
			items[i] = &page.Items[i]
		}
		return items, page.ResourceVersion, nil
	case "Node":
		var page struct {
			ResourceVersion uint64     `json:"resourceVersion"`
			Items           []api.Node `json:"items"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, 0, err
		}
		items := make([]api.Object, len(page.Items))
		for i := range page.Items {
			items[i] = &page.Items[i]
		}
		return items, page.ResourceVersion, nil
	case "ReplicaSet":
		var page struct {
			ResourceVersion uint64           `json:"resourceVersion"`
			Items           []api.ReplicaSet `json:"items"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, 0, err
		}
		items := make([]api.Object, len(page.Items))
		for i := range page.Items {
			items[i] = &page.Items[i]
		}
		return items, page.ResourceVersion, nil
	default:
		return nil, 0, fmt.Errorf("client: unknown kind %s", kind)
	}
}

func decodeEvent(kind string, line []byte) (registry.Event, error) {
	var raw struct {
		Type   registry.EventType `json:"type"`
		Object json.RawMessage    `json:"object"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return registry.Event{}, err
	}
	obj, err := newObject(kind)
	if err != nil {
		return registry.Event{}, err
	}
	if err := json.Unmarshal(raw.Object, obj); err != nil {
		return registry.Event{}, err
	}
	return registry.Event{Type: raw.Type, Object: obj}, nil
}
