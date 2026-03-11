// Package dbtunetest provides test infrastructure for the DBtune API.
//
// It serves two purposes:
//   - documents the surface area of the DBtune API that we use and rely on
//   - allows for testing DBtune API interactions so we can make changes with
//     confidence that we are not breaking things
package dbtunetest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
)

type Route [2]string

type Action struct {
	path       string
	method     string
	called     int
	testMethod func(*http.Request) (*http.Response, error)
}

func (action *Action) Call(req *http.Request) (*http.Response, error) {
	action.called++

	// belts and braces check that the action dispatch in the tests works as
	// intended and that this action is _only_ called for the intended requests
	if req.URL.Path != action.path || req.Method != action.method {
		return nil, fmt.Errorf("Action for %s:%s was called with path: %s, and method: %s", action.path, action.method, req.URL.Path, req.Method)
	}
	return action.testMethod(req)
}

// SuccessfulTrip is a RoundTripper that returns successful responses when its
// conditions are met.
type SuccessfulTrip struct {
	actions map[Route]*Action
}

func allActions() map[Route]*Action {
	return map[Route]*Action{
		{"/api/v1/agent/configurations", http.MethodGet}: {
			path:       "/api/v1/agent/configurations",
			method:     http.MethodGet,
			testMethod: getConfigurations,
		},
		{"/api/v1/agent/heartbeat", http.MethodPost}: {
			path:       "/api/v1/agent/heartbeat",
			method:     http.MethodPost,
			testMethod: postHeartbeat,
		},
		{"/api/v1/agent/configurations", http.MethodPost}: {
			path:       "/api/v1/agent/configurations",
			method:     http.MethodPost,
			testMethod: postActiveConfig,
		},
		{"/api/v1/agent/log-entries", http.MethodPost}: {
			path:       "/api/v1/agent/log-entries",
			method:     http.MethodPost,
			testMethod: postLogEntries,
		},
		{"/api/v1/agent/guardrails", http.MethodPost}: {
			path:       "/api/v1/agent/guardrails",
			method:     http.MethodPost,
			testMethod: postGuardrails,
		},
		{"/api/v1/agent/metrics", http.MethodPost}: {
			path:       "/api/v1/agent/metrics",
			method:     http.MethodPost,
			testMethod: postMetrics,
		},
		{"/api/v1/agent/system-info", http.MethodPut}: {
			path:       "/api/v1/agent/system-info",
			method:     http.MethodPut,
			testMethod: putSystemInfo,
		},
	}
}

func CreateSuccessfulTrip() SuccessfulTrip {
	all := allActions()
	actions := make(map[Route]*Action, len(all))
	for k, v := range all {
		actions[k] = v
	}
	return SuccessfulTrip{actions: actions}
}

// CreateSuccessfulTripWithRoutes creates a SuccessfulTrip containing only the
// specified routes. Use this when the code under test does not call all actions
// (e.g. the runner omits heartbeat and log-entries in a short test window) and
// you still want AllActionsCalled to pass.
func CreateSuccessfulTripWithRoutes(routes []Route) SuccessfulTrip {
	all := allActions()
	actions := make(map[Route]*Action, len(routes))
	for _, r := range routes {
		if a, ok := all[r]; ok {
			actions[r] = a
		}
	}
	return SuccessfulTrip{actions: actions}
}

func (st SuccessfulTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	action := st.actions[Route{req.URL.Path, req.Method}]
	if action != nil {
		return action.Call(req)
	}
	return nil, fmt.Errorf("Unrecognised Path: %s Method: %s", req.URL.Path, req.Method)
}

// AllActionsCalled asserts that every registered action was called at least once.
// Note that this will return false if you use it without defining any actions —
// it seems likely that either you will have made a mistake defining the actions
// or are calling this incorrectly. In both cases it is better to be told sooner.
func (st SuccessfulTrip) AllActionsCalled(t *testing.T) {
	if st.actions == nil {
		t.Error("No actions defined for transport.")
	}

	for _, action := range st.actions {
		if action.called == 0 {
			t.Errorf("Action %s was not called.", action.path)
			return
		}
	}
}

func (st SuccessfulTrip) ActionWasCalled(t *testing.T, path string, method string) {
	if st.actions == nil {
		t.Error("No actions defined for transport.")
		return
	}

	action := st.actions[Route{path, method}]

	if action == nil {
		t.Errorf("Action %s:%s was not defined.", path, method)
		return
	}

	if action.called == 0 {
		t.Errorf("Action %s:%s was not called.", path, method)
	}
}

func postHeartbeat(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 OK",
		StatusCode: http.StatusNoContent,
	}, nil
}

func postActiveConfig(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func getConfigurations(req *http.Request) (*http.Response, error) {
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}
	if req.URL.Query().Get("status") != "recommended" {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected status=recommended query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	body := `[{
		"config": [
			{"name": "shared_buffers", "setting": "256MB", "unit": "MB", "vartype": "string", "context": "postmaster"},
			{"name": "work_mem", "setting": "16MB", "unit": "MB", "vartype": "string", "context": "user"},
			{"name": "max_connections", "setting": 200, "unit": null, "vartype": "integer", "context": "postmaster"}
		],
		"knobs_overrides": ["shared_buffers"],
		"knob_application": "restart"
	}]`

	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}, nil
}

func postLogEntries(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func postGuardrails(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func postMetrics(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}

func putSystemInfo(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Content-Type") != "application/json" {
		return &http.Response{
			Status:     "415 Unsupported Media Type",
			StatusCode: http.StatusUnsupportedMediaType,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected Content-Type to be application/json, got %s", req.Header.Get("Content-Type")))),
		}, nil
	}
	if !req.URL.Query().Has("uuid") {
		return &http.Response{
			Status:     "400 Bad Request",
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf("Expected request to have uuid query parameter, raw query was: %s", req.URL.RawQuery))),
		}, nil
	}

	return &http.Response{
		Status:     "204 No Content",
		StatusCode: http.StatusNoContent,
	}, nil
}
