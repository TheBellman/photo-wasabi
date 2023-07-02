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
var octetMime = "binary/octet-stream"

func testFileReader(name string) io.ReadCloser {
	f, err := os.Open(name)
	if err != nil {
		log.Fatalf("Failed to open %s", name)
	}
	return f
}

func (f *mockSecretsClient) GetSecretValue(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error) {
	secret := "{\"ACCESS_KEY_ID\": \"key\", \"SECRET_ACCESS_KEY\": \"secret\"}"
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

	if *input.Key == "key/test.CR3" {
		return &s3.GetObjectOutput{
			ContentType: &octetMime,
			Body:        testFileReader("./test.CR3"),
		}, nil
	}

	if *input.Key == "key/test.HEIC" {
		return &s3.GetObjectOutput{
			ContentType: &octetMime,
			Body:        testFileReader("./test.HEIC"),
		}, nil
	}

	return nil, errors.New("unexpected test key provided")
}

func Test_getImageReader(t *testing.T) {
	mock := mockS3{}
	_, _, err := getImageReader(&mock, "bucket", "key/good.jpeg")
	if err != nil {
		t.Errorf("Received an unexpected error: %v", err)
	}

	_, _, err = getImageReader(&mock, "bucket", "key/bad.jpeg")
	if err == nil {
		t.Errorf("Did not get an error when expected")
	}

	_, _, err = getImageReader(&mock, "bucket", "key/test.CR3")
	if err != nil {
		t.Errorf("Received an unexpected error: %v", err)
	}
}

func Test_getImage(t *testing.T) {
	keys := []string{"./test.jpeg", "./test.CR3", "./test.HEIC"}
	for _, key := range keys {
		data, err := getImage(testFileReader(key))
		if err != nil {
			t.Errorf("unexpected error loading file: %v", err)
		}
		if len(*data) == 0 {
			t.Errorf("empty byte slice returned!")
		}
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

func Test_parseMessage(t *testing.T) {

	messageBody := `{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "eu-west-2",
      "eventTime": "2021-01-31T20:04:15.053Z",
      "eventName": "ObjectCreated:Copy",
      "userIdentity": {
        "principalId": "AWS:AROA46CDIYCJQTXNTWQ36:photo-lambda"
      },
      "requestParameters": {
        "sourceIPAddress": "35.177.37.71"
      },
      "responseElements": {
        "x-amz-request-id": "ADD9D43C5A604209",
        "x-amz-id-2": "gJp13URoEHybxQDe1eFpdX+IfB/LmUNPRsZdg7djTq/L1AtEHR3o0Ye5jvExrco94VGDLKAwfgjlrHgAApz5m3WVPeaDdT5RNP1Gv7wwiEQ="
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "tf-s3-topic-20210131162404270500000001",
        "bucket": {
          "name": "rahookphotos20200913140553484200000001",
          "ownerIdentity": {
            "principalId": "AM5JIJPPSMRC3"
          },
          "arn": "arn:aws:s3:::rahookphotos20200913140553484200000001"
        },
        "object": {
          "key": "photos/2020/03/20/IMG_0883.jpeg",
          "size": 10551027,
          "eTag": "f95f692e6caa5bb2fd9cfc66156d290c",
          "sequencer": "0060170D40D12F82DC"
        }
      }
    }
  ]
}
`

	message, err := parseMessage(messageBody)
	if err != nil {
		t.Errorf("parseMessage() error = %v", err)
	}

	for _, msg := range message.Records {
		if msg.S3.Bucket.Arn != "arn:aws:s3:::rahookphotos20200913140553484200000001" {
			t.Errorf("parseMessage() unexpected ARN: %s", msg.S3.Bucket.Arn)
		}
		if msg.S3.Object.Key != "photos/2020/03/20/IMG_0883.jpeg" {
			t.Errorf("parseMessage() unexpected key: %s", msg.S3.Object.Key)
		}
	}

}
