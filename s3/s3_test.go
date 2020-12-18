package s3logs

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	require.Panics(
		t,
		func() { New("us-east-1", "bogus-bucket", "bogus-prefix", "NOTANUMBER") },
		"Expected panic and didn't get it",
	)

	s3logs := New("us-east-1", "bogus-bucket", "bogus-prefix", "1")
	require.NotNil(t, s3logs)
}

func TestGetListOfFiles(t *testing.T) {
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("bogus1")},
				{Key: aws.String("bogus2")},
			},
		},
	}
	dlclient := &mockDLMgr{}
	s3logs := &S3Logs{
		s3client:      s3client,
		dlmgr:         dlclient,
		bucket:        "bogus-bucket",
		prefix:        "cf-logs/PREFIX",
		concurrency:   2,
		dlconcurrency: 5,
	}
	files, nextfile, err := s3logs.getListofFiles("bogus")
	require.NoError(t, err)
	require.Equal(t, nextfile, "bogus2")
	require.Equal(t, files, []*string{aws.String("bogus1"), aws.String("bogus2")})
}

func TestParseCFLogs(t *testing.T) {
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("bogus1")},
				{Key: aws.String("bogus2")},
			},
		},
	}
	dlclient := &mockDLMgr{}
	s3logs := &S3Logs{
		s3client:    s3client,
		dlmgr:       dlclient,
		bucket:      "bogus-bucket",
		prefix:      "cf-logs/PREFIX",
		concurrency: 2,
	}
	buffer := mockCompressedBuffer()
	_, err := s3logs.parseCFLogs(buffer)
	require.NoError(t, err)
}

func TestDownload(t *testing.T) {
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("bogus1")},
				//	{ Key: aws.String("bogus2") },
			},
		},
	}
	dlclient := &mockDLMgr{}

	s3logs := &S3Logs{
		s3client:    s3client,
		dlmgr:       dlclient,
		bucket:      "bogus-bucket",
		prefix:      "cf-logs/PREFIX",
		concurrency: 2,
	}
	_, _, err := s3logs.Download("bogus-startafterfile")
	require.NoError(t, err)
}

type mockS3Client struct {
	listobjectreturn *s3.ListObjectsV2Output
}

func (m *mockS3Client) ListObjectsV2(list *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return m.listobjectreturn, nil
}

type mockDLMgr struct{}

func (m *mockDLMgr) DownloadWithIterator(ctx aws.Context, iter s3manager.BatchDownloadIterator, opts ...func(*s3manager.Downloader)) error {
	wrbuffers := mockCompressedBuffer()
	var pos int64 = 0
	for _, wrbuffer := range wrbuffers {
		w, err := iter.DownloadObject().Writer.WriteAt(wrbuffer.buffer.Bytes(), pos)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		pos = int64(w)
	}
	return nil
}

func mockCompressedBuffer() (buffer []*wrbuffer) {
	var buf bytes.Buffer
	logtxt := "-"
	for i := 1; i < 34; i++ {
		logtxt = logtxt + "\t-"
	}
	logtxt = logtxt + "\n"
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(
		[]byte(logtxt)); err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	if err := zw.Close(); err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	awsbuf1 := aws.NewWriteAtBuffer(buf.Bytes())
	awsbuf2 := aws.NewWriteAtBuffer(buf.Bytes())
	buffer = []*wrbuffer{
		{
			filename: "bogus1",
			buffer:   awsbuf1,
		},
		{
			filename: "bogus2",
			buffer:   awsbuf2,
		},
	}
	return
}
