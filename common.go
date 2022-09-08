package s3rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	toServer = "to_server"
	toClient = "to_client"

	defaultRegion = "eu-north-1"

	// This gives us some time to determine if this is "our" message.
	// If so, we will delete it so that it is not processed again.
	visibilitySeconds = 7
)

type AWSConfig struct {
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
}

type common struct {
	tempDir string

	bucket string
	queue  string

	s3Client  *s3.Client
	sqsClient *sqs.Client

	closeOnce sync.Once

	infof func(format string, args ...interface{})
}

func (c *common) Receive(ctx context.Context) ([]message, error) {
	result, err := c.sqsClient.ReceiveMessage(ctx,
		&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queue),
			MaxNumberOfMessages: 5,
			VisibilityTimeout:   visibilitySeconds,
			// Wait for 20 seconds for a message to arrive.
			WaitTimeSeconds: 20,
		},
	)

	if err != nil {
		return nil, err
	}

	var messages []message
	for _, m := range result.Messages {
		var messageBody messageBody
		err := json.Unmarshal([]byte(*m.Body), &messageBody)
		if err != nil {
			return nil, err
		}
		if len(messageBody.Records) == 0 {
			continue
		}
		if len(messageBody.Records) > 1 {
			return nil, fmt.Errorf("expected only one record, got %d", len(messageBody.Records))
		}

		s3 := messageBody.Records[0].S3
		messages = append(messages, message{Bucket: s3.Bucket.Name, Key: s3.Object.Key, ReceiptHandle: *m.ReceiptHandle})
	}

	return messages, nil
}

func (c *common) deleteMessage(ctx context.Context, receiptHandle string) error {
	//c.infof("Delete message from %q", c.queue)
	_, err := c.sqsClient.DeleteMessage(
		ctx,
		&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.queue),
			ReceiptHandle: aws.String(receiptHandle),
		},
	)
	return err

}

func (c *common) deleteObject(ctx context.Context, key string) error {
	//c.infof("Delete %s/%s", c.bucket, key)
	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (c *common) getObject(ctx context.Context, f *os.File, key string) (map[string]string, error) {
	c.infof("Downloading %s/%s", c.bucket, key)
	o, err := c.s3Client.GetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(key),
		},
	)
	if err != nil {
		return nil, err
	}
	defer o.Body.Close()
	_, err = io.Copy(f, o.Body)
	if err != nil {
		return nil, err
	}
	return o.Metadata, nil

}

func (c *common) releaseMessage(ctx context.Context, receiptHandle string) error {
	//c.infof("Release message from %q", c.queue)
	_, err := c.sqsClient.ChangeMessageVisibility(
		ctx,
		&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(c.queue),
			ReceiptHandle:     aws.String(receiptHandle),
			VisibilityTimeout: 0,
		},
	)
	return err
}

func (c *common) upload(filename, key string, metaData map[string]string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	c.infof("Uploading %s to %s/%s", filename, c.bucket, key)

	metaDatap := make(map[string]*string)
	for k, v := range metaData {
		metaDatap[k] = aws.String(v)
	}

	_, err = manager.NewUploader(c.s3Client).Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:   aws.String(c.bucket),
		Key:      aws.String(key),
		Body:     file,
		Metadata: metaData,
	})

	if err != nil {
		return fmt.Errorf("upload: %v", err)
	}
	return nil
}

type message struct {
	Bucket        string
	Key           string
	ReceiptHandle string
}

type messageBody struct {
	Records []struct {
		EventVersion string    `json:"eventVersion"`
		EventSource  string    `json:"eventSource"`
		AwsRegion    string    `json:"awsRegion"`
		EventTime    time.Time `json:"eventTime"`
		EventName    string    `json:"eventName"`
		UserIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"userIdentity"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzRequestID string `json:"x-amz-request-id"`
			XAmzID2       string `json:"x-amz-id-2"`
		} `json:"responseElements"`
		S3 s3Object `json:"s3"`
	} `json:"Records"`
}

type s3Object struct {
	S3SchemaVersion string `json:"s3SchemaVersion"`
	ConfigurationID string `json:"configurationId"`
	Bucket          struct {
		Name          string `json:"name"`
		OwnerIdentity struct {
			PrincipalID string `json:"principalId"`
		} `json:"ownerIdentity"`
		Arn string `json:"arn"`
	} `json:"bucket"`
	Object struct {
		Key       string `json:"key"`
		Size      int    `json:"size"`
		ETag      string `json:"eTag"`
		Sequencer string `json:"sequencer"`
	} `json:"object"`
}
