package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

const (
	DefaultSrcPrefix = "photos/"
	DefaultRegion    = "eu-west-2"
	WasabiSecretName = "wasabi-access"
	MimeJPEG         = "image/jpeg"
	MimeHEIC         = "image/heic"
)

// App holds our dependencies and configuration
type App struct {
	Config     RuntimeConfig
	S3         *s3.Client
	Wasabi     *s3.Client
	BuildStamp string
}

type RuntimeConfig struct {
	WasabiBucket string
	WasabiRegion string
	SourcePrefix string
	Region       string
}

// NewApp initializes the application dependencies
func NewApp(ctx context.Context) (*App, error) {
	region := getEnv("AWS_REGION", DefaultRegion)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	app := &App{
		BuildStamp: os.Getenv("BUILD_STAMP"),
		Config: RuntimeConfig{
			Region:       region,
			SourcePrefix: validatePrefix(os.Getenv("SOURCE_PREFIX")),
			WasabiBucket: os.Getenv("WASABI_BUCKET"),
			WasabiRegion: os.Getenv("WASABI_REGION"),
		},
		S3: s3.NewFromConfig(cfg),
	}

	// Setup Wasabi
	smClient := secretsmanager.NewFromConfig(cfg)
	key, secret, err := app.getWasabiSecret(ctx, smClient)
	if err != nil {
		return nil, fmt.Errorf("failed to get wasabi credentials: %w", err)
	}

	wasabiCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(app.Config.WasabiRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		return nil, err
	}

	app.Wasabi = s3.NewFromConfig(wasabiCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("https://s3.%s.wasabisys.com", app.Config.WasabiRegion))
		o.UsePathStyle = true
	})

	return app, nil
}

func (a *App) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) (int, error) {
	processedCount := 0
	for _, record := range snsEvent.Records {
		var s3Event events.S3Event
		if err := json.Unmarshal([]byte(record.SNS.Message), &s3Event); err != nil {
			log.Printf("[%s] failed to parse SNS message: %v", a.BuildStamp, err)
			continue
		}

		for _, event := range s3Event.Records {
			if !a.shouldProcess(event) {
				continue
			}

			key, _ := url.QueryUnescape(event.S3.Object.Key)
			if err := a.processObject(ctx, event.S3.Bucket.Name, key); err != nil {
				log.Printf("[%s] error processing %s: %v", a.BuildStamp, key, err)
				continue
			}
			processedCount++
		}
	}
	return processedCount, nil
}

func (a *App) shouldProcess(event events.S3EventRecord) bool {
	return strings.HasPrefix(event.S3.Object.Key, a.Config.SourcePrefix) &&
		strings.HasPrefix(event.EventName, "ObjectCreated:")
}

func (a *App) processObject(ctx context.Context, bucket, key string) error {
	output, err := a.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer output.Body.Close()

	contentType := aws.ToString(output.ContentType)
	if !isSupported(key, contentType) {
		return fmt.Errorf("unsupported file type: %s (%s)", key, contentType)
	}

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return err
	}

	_, err = a.Wasabi.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.Config.WasabiBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

func (a *App) getWasabiSecret(ctx context.Context, client *secretsmanager.Client) (string, string, error) {
	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(WasabiSecretName),
	})
	if err != nil {
		return "", "", err
	}

	var creds struct {
		AccessKey string `json:"ACCESS_KEY_ID"`
		SecretKey string `json:"SECRET_ACCESS_KEY"`
	}
	if err := json.Unmarshal([]byte(*out.SecretString), &creds); err != nil {
		return "", "", err
	}
	return creds.AccessKey, creds.SecretKey, nil
}

func isSupported(key, contentType string) bool {
	lowerKey := strings.ToLower(key)
	return strings.HasSuffix(lowerKey, ".cr3") ||
		strings.HasSuffix(lowerKey, ".heic") ||
		contentType == MimeHEIC ||
		contentType == MimeJPEG
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func validatePrefix(p string) string {
	if p == "" {
		return DefaultSrcPrefix
	}
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}

func main() {
	ctx := context.Background()
	app, err := NewApp(ctx)
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}

	log.Printf("[%s] Starting photo-wasabi handler...", app.BuildStamp)
	lambda.Start(app.HandleLambdaEvent)
}
