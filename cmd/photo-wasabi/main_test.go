package main

import (
	"context"
	"encoding/json"
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

func Test_isSupported(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		contentType string
		want        bool
	}{
		{
			name:        "valid jpeg mime type",
			key:         "test.jpeg",
			contentType: MimeJPEG,
			want:        true,
		},
		{
			name:        "valid heic mime type",
			key:         "test",
			contentType: MimeHEIC,
			want:        true,
		},
		{
			name:        "valid cr3 extension",
			key:         "photos/image.cr3",
			contentType: "application/octet-stream",
			want:        true,
		},
		{
			name:        "valid uppercase orf extension",
			key:         "photos/image.ORF",
			contentType: "application/octet-stream",
			want:        true,
		},
		{
			name:        "valid heic extension",
			key:         "photos/image.heic",
			contentType: "application/octet-stream",
			want:        true,
		},
		{
			name:        "unsupported type",
			key:         "test.txt",
			contentType: "text/plain",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSupported(tt.key, tt.contentType); got != tt.want {
				t.Errorf("isSupported(%q, %q) = %v, want %v", tt.key, tt.contentType, got, tt.want)
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
