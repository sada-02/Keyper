package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sada-02/keyper/metrics"
)

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

// MetricsMiddleware instruments HTTP requests with Prometheus metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Normalize path for metrics (remove IDs)
		path := normalizePath(r.URL.Path)

		// Track in-flight requests
		reg := metrics.DefaultRegistry()
		reg.HTTPInFlight.WithLabelValues(r.Method, path).Inc()
		defer reg.HTTPInFlight.WithLabelValues(r.Method, path).Dec()

		// Wrap response writer to capture status code
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     200,
			written:        false,
		}

		// Call next handler
		next.ServeHTTP(rw, r)

		// Record metrics
		duration := time.Since(start)
		status := strconv.Itoa(rw.statusCode)
		reg.RecordHTTPRequest(r.Method, path, status, duration)
	})
}

// normalizePath converts paths with IDs to generic paths for metrics
// e.g., /v1/keys/mykey -> /v1/keys/:key
//
//	/v1/shards/0/_export -> /v1/shards/:id/_export
func normalizePath(path string) string {
	// Remove trailing slash
	path = strings.TrimSuffix(path, "/")

	if strings.HasPrefix(path, "/v1/keys/") {
		return "/v1/keys/:key"
	}

	if strings.HasPrefix(path, "/v1/shards/") {
		parts := strings.Split(path, "/")
		if len(parts) >= 4 {
			// /v1/shards/{id}/... -> /v1/shards/:id/...
			parts[3] = ":id"
			return strings.Join(parts, "/")
		}
	}

	if strings.HasPrefix(path, "/v1/control/shards/") {
		parts := strings.Split(path, "/")
		if len(parts) >= 5 {
			// /v1/control/shards/{id}/... -> /v1/control/shards/:id/...
			parts[4] = ":id"
			return strings.Join(parts, "/")
		}
	}

	if strings.HasPrefix(path, "/v1/control/nodes/") {
		return "/v1/control/nodes/:id"
	}

	return path
}
