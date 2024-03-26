package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
)

// Struct to represent the XML response body
type InitiateMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	UploadID string   `xml:"UploadId"`
}

// Struct to represent each part of the multipart upload
type Part struct {
	Number int
	ETag   string
}

func main() {
	// Specify your AWS credentials and region
	awsRegion := "us-east-2"
	awsAccessKeyID := "<AWS Access Key>"
	awsSecretAccessKey := "<AWS Secret Access Key>"

	// Specify the bucket name and object key
	bucketName := "<S3 Bucket>"
	objectKey := "<path/file>"

	// Specify the file path for uploading
	//filePath := "<path to file>/small.gz"
	filePath := "<path to file>/large.gz"

	// CreateMultipartUpload URL
	createMultipartUploadURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s?uploads", bucketName, awsRegion, objectKey)

	// Create an HTTP client
	client := http.DefaultClient

	// Create a new HTTP request for CreateMultipartUpload
	req, err := http.NewRequest("POST", createMultipartUploadURL, strings.NewReader(""))
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
		return
	}

	// Sign the request using AWS4-HMAC-SHA256 signer
	signer := v4.NewSigner(credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""))
	_, err = signer.Sign(req, nil, "s3", awsRegion, time.Now())
	if err != nil {
		fmt.Println("Error signing HTTP request:", err)
		return
	}

	// Set the necessary headers for authentication
	// req.Header.Set("x-amz-date", time.Now().UTC().Format(time.RFC3339))
	req.Header.Set("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))

	// req.Header.Set("Authorization", "AWS "+awsAccessKeyID+":"+awsSecretAccessKey)
	req.Header.Set("Host", req.URL.Host)

	fmt.Println("Starting the multipart upload ", time.Now())

	// Send the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return
	}
	defer resp.Body.Close()

	// Read the response body
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return
	}

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error:", resp.Status)
		fmt.Println("Response body:", string(responseBody))
		return
	}

	// Print the upload ID received in the response
	fmt.Println("Multipart upload created successfully. Upload ID:", string(responseBody))

	// Parse the XML response body
	var initResponse InitiateMultipartUploadResponse
	if err := xml.Unmarshal(responseBody, &initResponse); err != nil {
		fmt.Println("Error parsing XML:", err)
		return
	}

	// Print the upload ID received in the response
	fmt.Println("Multipart upload created successfully. Upload ID:", initResponse.UploadID)

	// Perform the multipart upload
	parts, err := performMultipartUpload(bucketName, objectKey, filePath, initResponse.UploadID, awsRegion, awsAccessKeyID, awsSecretAccessKey)
	if err != nil {
		fmt.Println("Error performing multipart upload:", err)
		return
	}

	// Complete the multipart upload
	if err := completeMultipartUpload(bucketName, objectKey, initResponse.UploadID, awsRegion, awsAccessKeyID, awsSecretAccessKey, parts); err != nil {
		fmt.Println("Error completing multipart upload:", err)
		return
	}

	fmt.Println("Multipart upload completed successfully.")

}

// Function to perform the multipart upload using multithreading
func performMultipartUpload(bucketName, objectKey, filePath, uploadID, awsRegion, awsAccessKeyID, awsSecretAccessKey string) ([]Part, error) {
	// Open the file to upload
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate the number of parts
	const partSize = 5 * 1024 * 1024 * 1024 // 5GB
	numParts := (fileSize + partSize - 1) / partSize

	// Create wait group to synchronize goroutines
	var wg sync.WaitGroup

	// Create channel for collecting uploaded parts
	partCh := make(chan Part, numParts)

	// Split the file into parts and upload each part concurrently
	for i := int64(0); i < numParts; i++ {
		wg.Add(1)
		partNumber := i + 1
		start := i * partSize
		end := (i + 1) * partSize
		if end > fileSize {
			end = fileSize
		}
		go func(partNumber int64, start, end int64) {
			defer wg.Done()

			// Read part of the file into buffer
			partData := make([]byte, end-start)
			_, err := file.ReadAt(partData, start)
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				return
			}

			// Upload the part
			eTag, err := uploadPart(bucketName, objectKey, uploadID, partNumber, partData, awsRegion, awsAccessKeyID, awsSecretAccessKey)
			if err != nil {
				fmt.Printf("Error uploading part %d: %v\n", partNumber, err)
				return
			}

			// Send the part number and ETag to the channel
			partCh <- Part{Number: int(partNumber), ETag: eTag}
		}(partNumber, start, end)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Close the part channel
	close(partCh)

	// Collect uploaded parts from channel
	var parts []Part
	for part := range partCh {
		parts = append(parts, part)
	}

	// Verify that all parts were uploaded
	if int64(len(parts)) != numParts {
		return nil, fmt.Errorf("error: expected %d parts, but only uploaded %d parts", numParts, len(parts))
	}

	return parts, nil
}

// Function to upload a part of the multipart upload
func uploadPart(bucketName, objectKey, uploadID string, partNumber int64, partData []byte, awsRegion, awsAccessKeyID, awsSecretAccessKey string) (string, error) {
	// UploadPart URL
	// uploadPartURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s?partNumber=%d&uploadId=%s", bucketName, awsRegion, objectKey, partNumber, uploadID)

	var uploadPartPresignedURL string
	var err error
	uploadPartPresignedURL, err = generatePresignedURL(bucketName, objectKey, uploadID, partNumber, awsRegion, awsAccessKeyID, awsSecretAccessKey)
	if err != nil {
		return "", fmt.Errorf("error creating presigned URL for HTTP request: %w", err)
	}

	// Create an HTTP client
	client := http.DefaultClient

	// Create a new HTTP request for UploadPart
	// req, err := http.NewRequest("PUT", uploadPartURL, bytes.NewReader(partData))
	req, err := http.NewRequest("PUT", uploadPartPresignedURL, bytes.NewReader(partData))
	if err != nil {
		return "", fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Sign the request using AWS4-HMAC-SHA256 signer
	signer := v4.NewSigner(credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""))
	_, err = signer.Sign(req, bytes.NewReader(partData), "s3", awsRegion, time.Now())
	if err != nil {
		return "", fmt.Errorf("error signing HTTP request: %w", err)
	}

	// Send the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("error reading response body: %w", err)
		}
		return "", fmt.Errorf("error uploading part %d: %s", partNumber, string(body))
	}

	// Read the response ETag header
	eTag := resp.Header.Get("ETag")
	if eTag == "" {
		return "", fmt.Errorf("error: ETag not found in response header")
	}

	return eTag, nil
}

// Function to complete the multipart upload
func completeMultipartUpload(bucketName, objectKey, uploadID, awsRegion, awsAccessKeyID, awsSecretAccessKey string, parts []Part) error {
	// Sort the parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Number < parts[j].Number
	})

	// CompleteMultipartUpload URL
	completeMultipartUploadURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s?uploadId=%s", bucketName, awsRegion, objectKey, uploadID)

	// Create an HTTP client
	client := http.DefaultClient

	// Generate the XML for completing the multipart upload
	xmlBody := "<CompleteMultipartUpload>"
	for _, part := range parts {
		xmlBody += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", part.Number, part.ETag)
	}
	xmlBody += "</CompleteMultipartUpload>"

	// Create a new HTTP request for CompleteMultipartUpload
	req, err := http.NewRequest("POST", completeMultipartUploadURL, strings.NewReader(xmlBody))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Sign the request using AWS4-HMAC-SHA256 signer
	signer := v4.NewSigner(credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""))
	_, err = signer.Sign(req, strings.NewReader(xmlBody), "s3", awsRegion, time.Now())
	if err != nil {
		return fmt.Errorf("error signing HTTP request: %w", err)
	}

	// Send the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %w", err)
		}
		return fmt.Errorf("error completing multipart upload: %s", string(body))
	}

	// Read the response body
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	return nil
}

// Function to generate presigned URL for uploading a part of the multipart upload
func generatePresignedURL(bucketName, objectKey, uploadID string, partNumber int64, awsRegion, awsAccessKeyID, awsSecretAccessKey string) (string, error) {
	// Construct the URL
	presignExpiration := time.Now().Add(15 * time.Minute) // Presigned URL valid for 15 minutes
	presignedURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s?partNumber=%d&uploadId=%s",
		bucketName, awsRegion, objectKey, partNumber, uploadID)

	// Create a new HTTP client
	// client := http.DefaultClient

	// Create a new HTTP request for presigned URL
	req, err := http.NewRequest("PUT", presignedURL, nil)
	if err != nil {
		return "", err
	}

	// Set the presigned URL expiration time
	req.Header.Set("X-Amz-Expires", "900") // 900 seconds (15 minutes)

	// Sign the request with AWS credentials
	signer := v4.NewSigner(credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""))
	_, err = signer.Sign(req, nil, "s3", awsRegion, presignExpiration)
	if err != nil {
		return "", err
	}

	// Get the presigned URL
	presignedURL = req.URL.String()

	return presignedURL, nil
}
