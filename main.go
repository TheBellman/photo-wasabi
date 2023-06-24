package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
)

// runtimeParameters contains various bits needed during execution
type runtimeParameters struct {
	WasabiKey     string
	WasabiSecret  string
	WasabiRegion  string
	WasabiBucket  string
	Region        string
	SourceBucket  string
	SourcePrefix  string
	S3service     *s3.S3
	WasabiService *s3.S3
}

// secretService helps with mocking access to SecretsManager
type secretService interface {
	GetSecretValue(input *secretsmanager.GetSecretValueInput) (*secretsmanager.GetSecretValueOutput, error)
}

// snsMessage encapsulates the message we get from SNS
type snsMessage struct {
	Records []events.S3EventRecord `json:"Records"`
}

// s3Service helps with mocking access to S3
type s3Service interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

var params *runtimeParameters
var buildStamp string

const (
	DefaultSrcPrefix = "photos/"
	DefaultRegion    = "eu-west-2"
	WasabiSecret     = "wasabi-access"
	JPEG             = "image/jpeg"
)

func init() {
	buildStamp = os.Getenv("BUILD_STAMP")

	params = &runtimeParameters{
		SourcePrefix: validatePrefix(os.Getenv("SOURCE_PREFIX")),
		Region:       validateRegion(os.Getenv("AWS_REGION")),
		WasabiBucket: os.Getenv("WASABI_BUCKET"),
		WasabiRegion: os.Getenv("WASABI_REGION"),
	}
}

// makeWasabiSession sets up a session to use with Wasabi.
func makeWasabiSession(region string, wasabiKey string, wasabiSecret string) (*session.Session, error) {
	config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(wasabiKey, wasabiSecret, ""),
		Endpoint:         aws.String(fmt.Sprintf("https://s3.%s.wasabisys.com", region)),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true),
	}

	return session.NewSession(config)
}

// makeAWSSession sets up an AWS session that can be used to connect to S3.
func makeAWSSession(region string) (*session.Session, error) {
	return session.NewSession(
		&aws.Config{
			Region: aws.String(region),
		})
}

// getWasabiSecret tries to fetch the key/secret pair for Wasabi access from SecretsManager.
func getWasabiSecret(client secretService) (key string, secret string, err error) {
	result, err := client.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretId: aws.String(WasabiSecret)})
	if err != nil {
		return "", "", fmt.Errorf("failed to read the wasabi secret  %v", err)
	}

	var values map[string]string
	err = json.Unmarshal([]byte(*result.SecretString), &values)
	if err != nil {
		return "", "", fmt.Errorf("failed to read the secret JSON %v", err)
	}

	return values["ACCESS_KEY_ID"], values["SECRET_ACCESS_KEY"], nil
}

// validateRegion will provide the default region if no region is set
func validateRegion(region string) string {
	if region == "" {
		return DefaultRegion
	} else {
		return region
	}
}

// validatePrefix coerces the environmental variable into a usable prefix, by adding a "/" if necessary or setting it to
// the default prefix. It returns the coerced prefix
func validatePrefix(photoPrefix string) string {
	if !strings.HasSuffix(photoPrefix, "/") {
		if photoPrefix == "" {
			photoPrefix = DefaultSrcPrefix
		} else {
			photoPrefix += "/"
		}
	}
	return photoPrefix
}

// getImageReader tries to get an io.Reader exposing the body of an image given the bucket and key. It will fail
// if the provided object is not a supported file type. It returns the reader along with the content type
func getImageReader(service s3Service, bucket string, key string) (io.Reader, string, error) {
	result, err := service.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", fmt.Errorf("error fetching from s3: %v", err)
	}

	if strings.HasSuffix(strings.ToLower(key), ".cr3") || *result.ContentType == JPEG {
		return result.Body, *result.ContentType, nil
	}

	return nil, "", fmt.Errorf("only JPEG and CR3 supported, fetched file %s was reported as %s",
		key,
		*result.ContentType)
}

// getImage retrieves the byte contents of a specified reader
func getImage(r io.Reader) (*[]byte, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return &[]byte{}, err
	}
	return &data, nil
}

// saveToWasabi uses the current configuration to write the supplied image bytes to the target key
// TODO: this needs a test
func saveToWasabi(params *runtimeParameters, image *[]byte, key string, contentType string) error {
	if params.WasabiService == nil {
		return fmt.Errorf("no service has been provided to write to wasabi")
	}

	result, err := params.WasabiService.PutObject(&s3.PutObjectInput{
		Body:        bytes.NewReader(*image),
		Bucket:      aws.String(params.WasabiBucket),
		ContentType: aws.String(contentType),
		Key:         aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("writing to wasabi failed: %v", err)
	}
	log.Printf("copied to wasabi %q with etag %q", key, *result.ETag)

	return nil
}

// parseMessage tries to forge the JSON message body from SNS
func parseMessage(messageBody string) (*snsMessage, error) {
	var message snsMessage

	err := json.Unmarshal([]byte(messageBody), &message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the message body: %q = %v", messageBody, err)
	}
	return &message, nil
}

// HandleLambdaEvent takes care of processing the incoming S3 event. Only "ObjectCreated:*" events are processed, and only
// for where the object key starts with the nominated prefix. The count of processed objects is returned
func HandleLambdaEvent(snsEvent events.SNSEvent) (int, error) {
	cnt := 0
	// each SNS event probably only has a single record in it, but you never know
	for _, record := range snsEvent.Records {
		message, err := parseMessage(record.SNS.Message)
		if err != nil {
			log.Printf("[%s] failed to parse the SNS message at all: %v", buildStamp, err)
			continue
		}

		for _, event := range message.Records {
			log.Printf("[%s] Received request for : object %s/%s", buildStamp, event.S3.Bucket.Name, event.S3.Object.Key)
			// only process events where the object key as the expected prefix and the event is an object creation
			if strings.HasPrefix(event.S3.Object.Key, params.SourcePrefix) && strings.HasPrefix(event.EventName, "ObjectCreated:") {
				decodedKey, err := url.QueryUnescape(event.S3.Object.Key)
				if err != nil {
					log.Printf("[%s] Failed to decode the key: '%s'", buildStamp, event.S3.Object.Key)
					continue
				}

				// this should be a cannot-happen case
				if event.AWSRegion != params.Region {
					log.Printf("[%s] Event is not from the same region as the lambda: got %q, wanted %q", buildStamp, event.AWSRegion, params.Region)
					continue
				}

				// fetch the object and hand back an io.reader
				imgReader, contentType, err := getImageReader(params.S3service, event.S3.Bucket.Name, decodedKey)
				if err != nil {
					log.Printf("[%s] Failed to get a reader to read from %s/%s: %v", buildStamp, event.S3.Bucket.Name, decodedKey, err)
					continue
				}

				// extract the image data
				imageBytes, err := getImage(imgReader)
				if err != nil {
					log.Printf("[%s] Failed to read image bytes: %v", buildStamp, err)
					continue
				}

				if err = saveToWasabi(params, imageBytes, decodedKey, contentType); err != nil {
					log.Printf("[%s] failed to copy to wasabi: %v", buildStamp, err)
					continue
				}

				log.Printf("[%s] Processed request for : object %s/%s", buildStamp, event.S3.Bucket.Name, decodedKey)
				cnt++
			}
		}
	}

	return cnt, nil
}

// main function invoked when the lambda is launched
func main() {
	// create a service to read from S3
	sess, err := makeAWSSession(params.Region)
	if err != nil {
		log.Fatal("Error starting AWS session", err)
	}
	params.S3service = s3.New(sess)

	// get the wasabi secrets from SecretsManager
	params.WasabiKey, params.WasabiSecret, err = getWasabiSecret(secretsmanager.New(sess))
	if err != nil {
		log.Fatal("Error fetching Wasabi key", err)
	}

	// and use the secrets to set up a service to write to Wasabi
	sess, err = makeWasabiSession(params.WasabiRegion, params.WasabiKey, params.WasabiSecret)
	if err != nil {
		log.Fatal("Error starting Wasabi session", err)
	}
	params.WasabiService = s3.New(sess)

	log.Printf("[%s] Registering handler for photo-wasabi...", buildStamp)
	lambda.Start(HandleLambdaEvent)
}
