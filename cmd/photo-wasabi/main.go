package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// Global variable to persist clients between warm starts
var app *App

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

// NewApp initializes the application dependencies including S3 and Wasabi clients.
// It returns an error if the AWS SDK configuration cannot be loaded or if
// Wasabi credentials cannot be retrieved.
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

// HandleLambdaEvent processes incoming SNS notifications and mirrors the
// referenced S3 objects to Wasabi storage.
//
// It returns the total count of processed objects and an error if the
// SNS message format is invalid.
func (a *App) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) (int, error) {
	processedCount := 0
	for _, record := range snsEvent.Records {
		var s3Event events.S3Event
		if err := json.Unmarshal([]byte(record.SNS.Message), &s3Event); err != nil {
			slog.Error("failed to parse SNS message",
				"build_stamp", a.BuildStamp,
				"error", err)
			continue
		}

		for _, event := range s3Event.Records {
			if !a.shouldProcess(event) {
				continue
			}

			key, _ := url.QueryUnescape(event.S3.Object.Key)
			if err := a.processObject(ctx, event.S3.Bucket.Name, key); err != nil {
				slog.Error("error processing object",
					"build_stamp", a.BuildStamp,
					"bucket", event.S3.Bucket.Name,
					"key", key,
					"error", err)
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

// processObject retrieves an object from the source S3 bucket and uploads it
// to the target Wasabi bucket using the same key.
func (a *App) processObject(ctx context.Context, bucket, key string) error {
	// If the Lambda context is cancelled (timeout), GetObject will return ctx.Err()
	output, err := a.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer output.Body.Close()

	contentType := aws.ToString(output.ContentType)
	if !isSupported(key, contentType) {
		return fmt.Errorf("unsupported file type: %s (%s)", key, contentType)
	}

	// Stream the data directly to Wasabi instead of ReadAll
	// The SDK will monitor the ctx and abort the PUT if the timeout is reached
	_, err = a.Wasabi.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(a.Config.WasabiBucket),
		Key:         aws.String(key),
		Body:        output.Body, // Pass the reader directly
		ContentType: aws.String(contentType),
		// ContentLength is recommended when streaming
		ContentLength: aws.Int64(aws.ToInt64(output.ContentLength)),
	})

	if err != nil {
		return fmt.Errorf("failed to put object to Wasabi: %w", err)
	}

	slog.Info("successfully mirrored object",
		"key", key,
		"size", aws.ToInt64(output.ContentLength),
		"content_type", contentType)

	return nil
}

// getWasabiSecret fetches the access/secret key pair used to authenticate to Wasabi
//
// It returns the access key, secret key and any errors.
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

// isSupported checks if the S3 key and object content type is supported for the application.
//
// It returns true if the filename and content type look ok.
func isSupported(key, contentType string) bool {
	lowerKey := strings.ToLower(key)
	return strings.HasSuffix(lowerKey, ".cr3") ||
		strings.HasSuffix(lowerKey, ".heic") ||
		contentType == MimeHEIC ||
		contentType == MimeJPEG
}

// getEnv fetches an environmental variable from the lambda environment. If not found
// it falls back on the provided fallback value.
//
// The variable value or the fallback string are returned.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// validatePrefix ensures that a valid prefix is available. If the supplied string to check is blank
// then the default source prefix is returned. If not blank, and it does not have a trailing /,
// then a trailing / is added.
//
// This returns a string which is an acceptable source prefix.
func validatePrefix(p string) string {
	if p == "" {
		return DefaultSrcPrefix
	}
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}

// main initializes the application, sets up logging, and starts the Lambda function handler.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx := context.Background()
	var err error
	app, err = NewApp(ctx)
	if err != nil {
		slog.Error("Initialization failed", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting photo-wasabi handler", "build_stamp", app.BuildStamp)
	lambda.Start(app.HandleLambdaEvent)
}
