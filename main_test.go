package main

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"io"
	"log"
	"os"
	"testing"
)

type mockSecretsClient struct{}
type mockS3 struct{}

var jpegMime = "image/jpeg"
var txtMime = "text/plain"

func testFileReader(name string) io.ReadCloser {
	f, err := os.Open(name)
	if err != nil {
		log.Fatalf("Failed to open %s", name)
	}
	return f
}

func (f *mockSecretsClient) GetSecretValue(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
	secret :=  "{\"ACCESS_KEY_ID\": \"key\", \"SECRET_ACCESS_KEY\": \"secret\"}"
	return &secretsmanager.GetSecretValueOutput{
		SecretString: &secret,
	}, nil
}

func (f *mockS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *input.Key == "key/good.jpeg" {
		return &s3.GetObjectOutput{
			ContentType: &jpegMime,
			Body:        testFileReader("./test.jpeg"),
		}, nil
	}

	if *input.Key == "key/bad.jpeg" {
		return &s3.GetObjectOutput{
			ContentType: &txtMime,
			Body:        testFileReader("./test.jpeg"),
		}, nil
	}

	return nil, errors.New("unexpected test key provided")
}

func Test_getImageReader(t *testing.T) {
	mock := mockS3{}
	_, err := getImageReader(&mock, "bucket", "key/good.jpeg")
	if err != nil {
		t.Errorf("Received an unexpected error: %v", err)
	}

	_, err = getImageReader(&mock, "bucket", "key/bad.jpeg")
	if err == nil {
		t.Errorf("Did not get an error when expected")
	}
}

func Test_getImage(t *testing.T) {
	data, err := getImage(testFileReader("./test.jpeg"))
	if err != nil {
		t.Errorf("unexpected error loading file: %v", err)
	}
	if len(*data) == 0 {
		t.Errorf("empty byte slice returned!")
	}
}

func Test_getWasabiSecret(t *testing.T) {
	mock := mockSecretsClient{}
	key, secret, err := getWasabiSecret(&mock)
	if err != nil {
		t.Errorf("getWasabiSecret() : %v", err)
	}

	if key != "key" || secret != "secret" {
		t.Errorf("wanted %q, %q, got %q, %q", "key", "secret", key, secret)
	}
}

func Test_validatePrefix(t *testing.T) {
	type args struct {
		photoPrefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "empty", args: args{photoPrefix: ""}, want: DefaultSrcPrefix},
		{name: "nonempty", args: args{photoPrefix: "folder"}, want: "folder/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validatePrefix(tt.args.photoPrefix); got != tt.want {
				t.Errorf("validatePrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_validateRegion(t *testing.T) {
	type args struct {
		region string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "empty", args: args{region: ""}, want: DefaultRegion},
		{name: "nonempty", args: args{region: "us-east-1"}, want: "us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validateRegion(tt.args.region); got != tt.want {
				t.Errorf("validateRegion() = %v, want %v", got, tt.want)
			}
		})
	}
}
