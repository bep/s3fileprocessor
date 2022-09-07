package s3rpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// NewClient creates a new client.
func NewClient(opts ClientOptions) (*Client, error) {
	if err := opts.init(); err != nil {
		return nil, err
	}

	awsCfg := aws.Config{
		Region:      opts.Region,
		Credentials: credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, ""),
	}

	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Minute
	}

	if opts.Infof == nil {
		opts.Infof = func(format string, args ...interface{}) {
			fmt.Println("client: " + fmt.Sprintf(format, args...))
		}
	}

	tempDir, err := os.MkdirTemp("", "s3rpc_client")
	if err != nil {
		return nil, err
	}

	return &Client{
		timeout: opts.Timeout,
		common: &common{
			bucket:    opts.Bucket,
			queue:     opts.Queue,
			s3Client:  s3.NewFromConfig(awsCfg),
			sqsClient: sqs.NewFromConfig(awsCfg),
			tempDir:   tempDir,
			infof:     opts.Infof,
		},
	}, nil

}

// Client is a client for executing operations on a server.
type Client struct {
	timeout time.Duration
	*common
}

// ExecuteFilename executes the given op on a server with filename as its input.
// This will block until the response is received or the timeout is reached.
// Note that Output.Filename should be considered temporary and will be removed on Close.
func (c *Client) ExecuteFilename(ctx context.Context, op, filename string) (Output, error) {
	id := uuid.New().String()
	key := fmt.Sprintf("%s/%s/%s_%s", toServer, op, id, filepath.Base(filename))

	// First upload the file to the input folder.
	if err := c.upload(filename, key, nil); err != nil {
		return Output{}, fmt.Errorf("apply: %v", err)
	}

	var output Output

	// Now, wait for the response from server.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				//c.infof("Checking queue %q for new messages", c.queue)
				ms, err := c.Receive(ctx)
				if err != nil {
					return err
				}
				for _, m := range ms {
					if m.Bucket != c.bucket {
						return fmt.Errorf("expected bucket %q, got %q", c.bucket, m.Bucket)
					}

					if !strings.Contains(m.Key, id) {
						if err := c.releaseMessage(ctx, m.ReceiptHandle); err != nil {
							return err
						}
						continue
					}

					// We found the message we are looking for.
					// Delete the message from the queue and download the file from S3.
					if err := c.deleteMessage(ctx, m.ReceiptHandle); err != nil {
						return err
					}

					return func() error {
						f, err := os.CreateTemp(c.tempDir, "*_"+path.Base(m.Key))
						if err != nil {
							return fmt.Errorf("tempfile: %w", err)
						}
						output.Filename = f.Name()
						defer f.Close()

						c.infof("Download %q", m.Key)
						metaData, err := c.getObject(ctx, f, m.Key)
						if err != nil {
							return err
						}
						output.Metadata = metaData

						// We don't need these anymore.
						// They will eventually also expire,
						// if the below should somehow fail,
						// so ignore any error.
						_ = c.deleteObject(ctx, m.Key)
						_ = c.deleteObject(ctx, key)
						return nil
					}()
				}
			}
		}
	})

	if err := g.Wait(); err != nil {
		return Output{}, fmt.Errorf("apply: %v", err)
	}

	return output, nil

}

// Close removes the temporary directory.
func (c *Client) Close() error {
	return os.RemoveAll(c.tempDir)
}

type ClientOptions struct {
	// The out queue to listen for responses from server.
	Queue string

	// Timeout is the maximum time to wait for a response from the server.
	Timeout time.Duration

	// Infof logs info messages.
	Infof func(format string, args ...interface{})

	// The AWS config.
	AWSConfig
}

func (opts *ClientOptions) init() error {
	if opts.Region == "" {
		opts.Region = defaultRegion
	}

	if opts.AccessKeyID == "" {
		return errors.New("access key id is required")
	}

	if opts.SecretAccessKey == "" {
		return errors.New("secret access key is required")
	}

	if opts.Queue == "" {
		return fmt.Errorf("queue is required")
	}

	return nil
}
