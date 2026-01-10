package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3 struct {
	getObjectFunc func(ctx context.Context, params *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (m *mockS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getObjectFunc(ctx, params)
}

func testFileReader(name string) io.ReadCloser {
	f, err := os.Open(name)
	if err != nil {
		log.Fatalf("Failed to open %s: %v", name, err)
	}
	return f
}

func TestApp_processObject(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		contentType string
		wantErr     bool
	}{
		{
			name:        "valid jpeg",
			key:         "test.jpeg",
			contentType: "image/jpeg",
			wantErr:     false,
		},
		{
			name:        "unsupported type",
			key:         "test.txt",
			contentType: "text/plain",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSupported(tt.key, tt.contentType)
			if tt.wantErr && got {
				t.Errorf("isSupported(%q, %q) = true, want false", tt.key, tt.contentType)
			}
			if !tt.wantErr && !got {
				t.Errorf("isSupported(%q, %q) = false, want true", tt.key, tt.contentType)
			}
		})
	}
}

func Test_validatePrefix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", DefaultSrcPrefix},
		{"folder", "folder/"},
		{"folder/", "folder/"},
	}

	for _, tt := range tests {
		if got := validatePrefix(tt.input); got != tt.want {
			t.Errorf("validatePrefix(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func Test_HandleLambdaEvent_Parsing(t *testing.T) {
	app := &App{BuildStamp: "test"}

	s3Record := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "src"},
					Object: events.S3Object{Key: "photos/img.jpg"},
				},
			},
		},
	}
	s3Json, _ := json.Marshal(s3Record)

	event := events.SNSEvent{
		Records: []events.SNSEventRecord{
			{
				SNS: events.SNSEntity{
					Message: string(s3Json),
				},
			},
		},
	}

	// This tests that the parsing logic inside the handler works
	// You can mock the internal processObject call to verify behavior
	count, err := app.HandleLambdaEvent(context.TODO(), event)
	if err != nil {
		t.Errorf("HandleLambdaEvent returned error: %v", err)
	}
	if count != 0 { // Should be 0 because S3/Wasabi clients are nil in this test
		t.Errorf("Expected 0 processed records, got %d", count)
	}
}
