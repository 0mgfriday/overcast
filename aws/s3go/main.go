package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	bucketName := flag.String("b", "", "s3 bucket name")
	prefix := flag.String("p", "", "prefix")
	exclusionsString := flag.String("e", "", "file extensions to exclude (comma seperated) ex: 'png,jpg,jpeg'")

	flag.Parse()

	if *bucketName == "" {
		fmt.Println("bucket name (-b) required")
		os.Exit(2)
	}

	if *prefix != "" && !strings.HasSuffix(*prefix, "/") {
		*prefix += "/"
	}

	exclusions := []string{}
	if *exclusionsString != "" {
		exclusions = strings.Split(*exclusionsString, ",")
	}

	region, getRegionErr := getRegion(*bucketName)
	if getRegionErr == nil && region != "" {
		fmt.Println("Bucket:", *bucketName)
		fmt.Println("Region", region)

		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(aws.AnonymousCredentials{}),
			config.WithDefaultRegion(region),
		)

		if err != nil {
			log.Fatal(err)
		}

		client := s3.NewFromConfig(cfg)

		s3Enum := s3Enum{
			client:     *client,
			exclusions: exclusions,
		}

		s3Enum.listWithPrefix(*bucketName, *prefix)
	} else {
		fmt.Println(getRegionErr)
	}
}

func getRegion(bucketName string) (string, error) {
	requestURL := fmt.Sprintf("https://%s.s3.amazonaws.com", bucketName)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	res, err := client.Get(requestURL)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
		return "", err
	}
	region := res.Header.Get("x-amz-bucket-region")
	if region == "" {
		return "", errors.New("missing region header")
	}

	return region, nil
}

type s3Enum struct {
	exclusions []string
	client     s3.Client
}

func (e s3Enum) listWithPrefix(bucket string, prefix string) {
	output, err := e.client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	if err == nil {
		for _, object := range output.CommonPrefixes {
			e.listWithPrefix(bucket, *object.Prefix)
		}

		for _, object := range output.Contents {
			extension := strings.Replace(filepath.Ext(aws.ToString(object.Key)), ".", "", 1)
			if !slices.Contains(e.exclusions, extension) {
				fmt.Printf("%s\t%d\t%s\n", object.LastModified, *object.Size, aws.ToString(object.Key))
			}
		}
	} else {
		fmt.Println("\t\t\t\t\t" + prefix + " (Access Denied)")
	}
}
