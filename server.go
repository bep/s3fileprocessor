package s3rpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"golang.org/x/sync/errgroup"
)

// NewServer creates a new server.
func NewServer(opts ServerOptions) (*Server, error) {
	if err := opts.init(); err != nil {
		return nil, err
	}

	awsCfg := aws.Config{
		Region:      opts.Region,
		Credentials: credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, ""),
	}

	if opts.PollInterval == 0 {
		opts.PollInterval = 10 * time.Second
	}

	if opts.Infof == nil {
		opts.Infof = func(format string, args ...interface{}) {
			fmt.Println("server: " + fmt.Sprintf(format, args...))
		}
	}

	tempDir, err := os.MkdirTemp("", "s3rpc_server")
	if err != nil {
		return nil, err
	}

	return &Server{
		handlers:      opts.Handlers,
		pollIntervall: opts.PollInterval,
		quit:          make(chan struct{}),
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

// Output is the result of a handler invocation.
type Output struct {
	Filename string
	Metadata map[string]string
}

// Input is the input to a handler invocation.
type Input struct {
	Filename string
	Metadata map[string]string
}

// Handlers is a map of operation names to handler functions.
type Handlers map[string]func(ctx context.Context, input Input) (Output, error)

// Server is a server that processes files from an S3 bucket.
type Server struct {
	handlers      Handlers
	pollIntervall time.Duration
	quit          chan struct{}
	*common
}

// Close closes the server.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.quit)
		err = os.RemoveAll(s.tempDir)
	})
	return err
}

// ListenAndServe listens for messages and processes them.
// It blocks until the server is closed.
func (s *Server) ListenAndServe(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-s.quit:
				s.infof("Closed")
				return nil
			case <-ctx.Done():
				return nil
			default:
				s.infof("Checking queue %q for new messages", s.queue)
				ms, err := s.Receive(ctx)
				if err != nil {
					return err
				}

				for _, m := range ms {
					if m.Bucket != s.bucket {
						return fmt.Errorf("expected bucket %q, got %q", s.bucket, m.Bucket)
					}

					s.infof("Got message with key %q", m.Key)

					op := strings.Split(m.Key, "/")[1]
					handle := s.handlers[op]
					if handle == nil {
						if err := s.releaseMessage(ctx, m.ReceiptHandle); err != nil {
							return err
						}
						continue
					}

					// We have a handler for this operation, so we can process the file.
					// Delete the message from the queue before the visibility timeout expires.
					if err := s.deleteMessage(ctx, m.ReceiptHandle); err != nil {
						return err
					}

					baseKey := path.Base(m.Key)

					err = func() error {
						f, err := os.CreateTemp(s.tempDir, "*_"+baseKey)
						if err != nil {
							return fmt.Errorf("tempfile: %w", err)
						}
						defer f.Close()
						defer os.Remove(f.Name())

						metaData, err := s.getObject(ctx, f, m.Key)
						if err != nil {
							return err
						}

						result, err := handle(ctx, Input{Filename: f.Name(), Metadata: metaData})
						if err != nil {
							return fmt.Errorf("handle: %w", err)
						}

						// The client uses an UUID in the base name of the file to identify the
						// message in the output quueue, so we need to preserve that.
						// With that, we also know that it's unique.
						key := toClient + "/" + op + "/" + baseKey

						if err := s.upload(result.Filename, key, result.Metadata); err != nil {
							return err
						}

						return err

					}()

					if err != nil {
						return err
					}
				}

				time.Sleep(s.pollIntervall)
			}
		}
	})

	return g.Wait()

}

// ServerOptions are options for the server.
type ServerOptions struct {
	// Handlers maps an operation to a handler.
	// The operation is also the first path segment below in/out in the bucket.
	Handlers Handlers

	// The in queue to poll for new messages.
	Queue string

	// PollInterval is the interval between polling for new messages.
	PollInterval time.Duration

	// Infof logs info messages.
	Infof func(format string, args ...interface{})

	// The AWS config.
	AWSConfig
}

func (opts *ServerOptions) init() error {
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
