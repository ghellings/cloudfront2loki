package s3logs

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func TestNew(t *testing.T) {
	s3logs := New("us-east-1", "bogus", "bogus", "1")
	if s3logs == nil {
		t.Errorf("Expected s3logs to not be nil\n")
	}
}

func TestGetListOfFiles(t *testing.T) {
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{},
	}
	dlclient := &mockDLMgr{}
	s3logs := &S3Logs{
		s3client:    s3client,
		dlmgr:       dlclient,
		bucket:      "b7i-sumologic",
		prefix:      "cf-logs/E1OUPXPV64DT62",
		concurrency: 2,
	}
	_, _, err := s3logs.getListofFiles("bogus")
	if err != nil {
		t.Errorf("Expected no error, got: %s\n", err)
	}
}

func TestParseCFLogs(t *testing.T) {
	s3logs := New("us-east-1", "bogus", "bogus", "1")
	buffer := []*aws.WriteAtBuffer{}
	_, err := s3logs.parseCFLogs(buffer)
	if err != nil {
		t.Errorf("Expected no error, got: %s\n", err)
	}
}

func TestDownload(t *testing.T) {
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{},
	}
	dlclient := &mockDLMgr{}

	s3logs := &S3Logs{
		s3client:    s3client,
		dlmgr:       dlclient,
		bucket:      "b7i-sumologic",
		prefix:      "cf-logs/E1OUPXPV64DT62",
		concurrency: 2,
		startafter:  "cf-logs/E1OUPXPV64DT62.2019-12-04-16.3c39d514.gz",
	}
	_, _, err := s3logs.Download()
	if err != nil {
		t.Errorf("Expected no error, got: %s\n", err)
	}
}

type mockS3Client struct {
	listobjectreturn *s3.ListObjectsV2Output
}

func (m *mockS3Client) ListObjectsV2(list *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return m.listobjectreturn, nil
}

type mockDLMgr struct{}

func (m *mockDLMgr) DownloadWithIterator(aws.Context, s3manager.BatchDownloadIterator, ...func(*s3manager.Downloader)) error {
	return nil
}
