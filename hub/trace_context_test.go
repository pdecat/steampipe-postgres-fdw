package hub

import (
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestParseTraceContext(t *testing.T) {
	hub := &hubBase{}

	tests := []struct {
		name     string
		input    string
		expected bool // whether valid context should be extracted
	}{
		{
			name:     "Valid traceparent only",
			input:    "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			expected: true,
		},
		{
			name:     "Valid traceparent and tracestate",
			input:    "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01;tracestate=rojo=00f067aa0ba902b7",
			expected: true,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "Invalid format",
			input:    "invalid-trace-context",
			expected: false,
		},
		{
			name:     "Missing traceparent",
			input:    "tracestate=rojo=00f067aa0ba902b7",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := hub.parseTraceContext(tt.input)
			hasValidSpan := ctx != nil && trace.SpanContextFromContext(ctx).IsValid()

			if hasValidSpan != tt.expected {
				t.Errorf("parseTraceContext() = %v, expected %v for input: %s", hasValidSpan, tt.expected, tt.input)
			}

			if hasValidSpan {
				spanCtx := trace.SpanContextFromContext(ctx)
				t.Logf("Successfully extracted trace context - TraceID: %s, SpanID: %s",
					spanCtx.TraceID().String(), spanCtx.SpanID().String())
			}
		})
	}
}

func TestTraceContextForScanWithSessionVariables(t *testing.T) {
	hub := &hubBase{}

	// Test with trace context in options
	opts := map[string]string{
		"connection":    "aws",
		"table":         "aws_s3_bucket",
		"trace_context": "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	traceCtx := hub.traceContextForScan("aws_s3_bucket", []string{"name", "region"}, 10, nil, "aws", opts)

	if traceCtx == nil {
		t.Fatal("Expected trace context to be created")
	}

	if traceCtx.Span == nil {
		t.Fatal("Expected span to be created")
	}

	// Verify that the span has a valid context
	spanCtx := traceCtx.Span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("Expected span context to be valid")
	}

	t.Logf("Created span with TraceID: %s, SpanID: %s",
		spanCtx.TraceID().String(), spanCtx.SpanID().String())
}

func TestTraceContextForScanWithoutSessionVariables(t *testing.T) {
	hub := &hubBase{}

	// Test without trace context in options
	opts := map[string]string{
		"connection": "aws",
		"table":      "aws_s3_bucket",
	}

	traceCtx := hub.traceContextForScan("aws_s3_bucket", []string{"name", "region"}, 10, nil, "aws", opts)

	if traceCtx == nil {
		t.Fatal("Expected trace context to be created")
	}

	if traceCtx.Span == nil {
		t.Fatal("Expected span to be created")
	}

	// Verify that the span has a valid context (should be a new root span)
	spanCtx := traceCtx.Span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("Expected span context to be valid")
	}

	t.Logf("Created root span with TraceID: %s, SpanID: %s",
		spanCtx.TraceID().String(), spanCtx.SpanID().String())
}
