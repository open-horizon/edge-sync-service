package communications

import (
	"context"
	"net/http"
	"sync"
)

type httpRequestWrapper struct {
	httpClient http.Client
	inFlight   map[*http.Request]context.CancelFunc
	lock       sync.Mutex
}

func newHTTPRequestWrapper(httpClient http.Client) *httpRequestWrapper {
	wrapper := httpRequestWrapper{httpClient: httpClient, inFlight: make(map[*http.Request]context.CancelFunc)}
	return &wrapper
}

func (wrapper *httpRequestWrapper) do(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(context.Background())
	reqWithCtx := request.WithContext(ctx)

	wrapper.lock.Lock()
	wrapper.inFlight[reqWithCtx] = cancel
	wrapper.lock.Unlock()

	response, err := wrapper.httpClient.Do(reqWithCtx)

	wrapper.lock.Lock()
	delete(wrapper.inFlight, reqWithCtx)
	wrapper.lock.Unlock()

	return response, err
}

func (wrapper *httpRequestWrapper) cancel() {
	wrapper.lock.Lock()
	for request, cancel := range wrapper.inFlight {
		cancel()
		delete(wrapper.inFlight, request)
	}
	wrapper.lock.Unlock()
}
