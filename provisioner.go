package s3rpc

import (
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/bep/awscreate"
	"github.com/bep/awscreate/s3rpccreate"
)

// NewProvisioner returns a new Provisioner that can be used to create and destroy an AWS environment
// with all the users, buckets and queues needed for s3rpc.
// Pass the result into PrintProvisionResults.
func NewProvisioner(name, region string) (awscreate.Provisioner[s3rpccreate.CreateResults], error) {
	keyID := os.Getenv("S3RPC_ADMIN_ACCESS_KEY_ID")
	keySecret := os.Getenv("S3RPC_ADMIN_ACCESS_KEY_SECRET")

	if keyID == "" || keySecret == "" {
		return nil, errors.New("S3RPC_ADMIN_ACCESS_KEY_ID and S3RPC_ADMIN_ACCESS_KEY_SECRET must be set")
	}

	awsCfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(keyID, keySecret, ""),
	}

	return s3rpccreate.New(
		s3rpccreate.Options{
			AdminCfg: awsCfg,
			Name:     name,
			Region:   region,
		}), nil

}

// PrintProvisionResults prints the config releventa parts of the provision results to stdout,
// sutiable for sourcing in a shell script.
func PrintProvisionResults(outputs s3rpccreate.CreateResults) {
	for i, q := range outputs.Queues {
		name := "CLIENT"
		if i == 1 {
			name = "SERVER"
		}
		fmt.Printf("S3RPC_%s_QUEUE=%s\n", name, *q.QueueUrl)
	}

	for i, k := range outputs.AccessKeys {
		name := "CLIENT"
		if i == 1 {
			name = "SERVER"
		}
		fmt.Printf("S3RPC_%s_ACCESS_KEY_ID=%s\n", name, *k.AccessKey.AccessKeyId)
		fmt.Printf("S3RPC_%s_SECRET_ACCESS_KEY=%s\n", name, *k.AccessKey.SecretAccessKey)
	}
}
