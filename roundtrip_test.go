package s3rpc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"golang.org/x/sync/errgroup"
)

func TestProcessFile(t *testing.T) {
	c := qt.New(t)

	infofc := func(format string, args ...interface{}) {
		fmt.Println("client: " + fmt.Sprintf(format, args...))
	}

	client, err := NewClient(
		ClientOptions{
			Queue:   os.Getenv("S3RPC_CLIENT_QUEUE"),
			Timeout: 5 * time.Minute,
			Infof:   infofc,
			AWSConfig: AWSConfig{
				Bucket:          "s3fptest",
				AccessKeyID:     os.Getenv("S3RPC_CLIENT_ACCESS_KEY_ID"),
				SecretAccessKey: os.Getenv("S3RPC_CLIENT_SECRET_ACCESS_KEY"),
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(client, qt.Not(qt.IsNil))

	infofs := func(format string, args ...interface{}) {
		fmt.Println("server: " + fmt.Sprintf(format, args...))
	}

	changedString := fmt.Sprintf("___changed__%d", time.Now().UnixNano())

	handlers := Handlers{
		"dosomething": func(ctx context.Context, input Input) (Output, error) {
			infofs("dosomething: %s", input.Filename)
			b, err := os.ReadFile(input.Filename)
			if err != nil {
				return Output{}, err
			}
			newContent := string(b) + "\n\n" + changedString
			ext := filepath.Ext(input.Filename)
			newFilename := strings.TrimSuffix(input.Filename, ext) + "-changed" + ext
			if err := os.WriteFile(newFilename, []byte(newContent), 0644); err != nil {
				return Output{}, err
			}

			return Output{
				Filename: newFilename,
				Metadata: map[string]string{
					"foo": "bar",
				},
			}, nil

		},
	}

	server, err := NewServer(
		ServerOptions{
			Handlers: handlers,
			Queue:    os.Getenv("S3RPC_SERVER_QUEUE"),
			Infof:    infofs,
			AWSConfig: AWSConfig{
				Bucket:          "s3fptest",
				AccessKeyID:     os.Getenv("S3RPC_SERVER_ACCESS_KEY_ID"),
				SecretAccessKey: os.Getenv("S3RPC_SERVER_SECRET_ACCESS_KEY"),
			},
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(server, qt.Not(qt.IsNil))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gServer, sctx := errgroup.WithContext(ctx)
	gClient, cctx := errgroup.WithContext(ctx)

	gServer.Go(func() error {
		return server.ListenAndServe(sctx)
	})

	wd, err := os.Getwd()
	c.Assert(err, qt.IsNil)

	gClient.Go(func() error {
		res, err := client.Execute(cctx, "dosomething", Input{Filename: filepath.Join(wd, "go.mod")})
		if err != nil {
			return err
		}
		infofc("filename: %s", res.Filename)
		b, err := os.ReadFile(res.Filename)
		if err != nil {
			return err
		}
		s := string(b)
		if !strings.Contains(s, changedString) {
			return fmt.Errorf("expected to find %s in %q", changedString, s)
		}
		if res.Metadata["foo"] != "bar" {
			return fmt.Errorf("expected metadata to contain foo=bar, got %v", res.Metadata)
		}
		return err
	})

	c.Assert(gClient.Wait(), qt.IsNil)
	c.Assert(server.Close(), qt.IsNil)
	c.Assert(gServer.Wait(), qt.IsNil)
	c.Assert(client.Close(), qt.IsNil)
}

func TestSetup(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping in CI")
	}

	prov, err := NewProvisioner("s3fpdev", defaultRegion) // s3fptest is used on GitHub
	if err != nil {
		t.Fatal(err)
	}

	if err := prov.Destroy(context.Background()); err != nil {
		t.Fatal(err)
	}

	outputs, err := prov.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	PrintProvisionResults(outputs)

}
