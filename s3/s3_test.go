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
	s3logs := mocks3logsclient()
	files, nextfile, err := s3logs.getListofFiles("cf-logs/PREFIX", "bogus")
	require.NoError(t, err)
	require.Equal(t, nextfile, "bogus2")
	require.Equal(t, files, []*string{aws.String("bogus1"), aws.String("bogus2")})
}

func TestParseCFLogs(t *testing.T) {
	s3logs := mocks3logsclient()
	buffer := mockCompressedBuffer()
	_, err := s3logs.parseCFLogs(buffer)
	require.NoError(t, err)
}

func TestDownload(t *testing.T) {
	s3logs := mocks3logsclient()
	_, _, err := s3logs.Download("bogus-startafterfile")
	require.NoError(t, err)
}

func TestDownLoadFiles(t *testing.T) {
	s3logs := mocks3logsclient()
	files := []*string{
		aws.String("bogus1"),
		aws.String("bogus2"),
	}
	_, err := s3logs.downLoadFiles(files)
	require.NoError(t, err)
}

func TestWatchBucket(t *testing.T) {
	// Test with no files to download
	s3logs := mocks3logsclient()
	pulledfiles := map[string]int{
		"bogus1": 1,
		"bogus2": 1,
	}
	cfloglines, pulledfiles, err := s3logs.WatchBucket("cf-logs/PREFIX", pulledfiles)
	require.NoError(t, err)
	require.Equal(t, 1, pulledfiles["bogus1"])
	require.Equal(t, 1, pulledfiles["bogus2"])
	require.Equal(t, 0, len(cfloglines))

	// Test with 2 files to download
	s3logs = mocks3logsclient()
	pulledfiles = map[string]int{}
	_, pulledfiles, err = s3logs.WatchBucket("cf-logs/PREFIX", pulledfiles)
	require.NoError(t, err)
	require.Equal(t, 1, pulledfiles["bogus1"])
	require.Equal(t, 1, pulledfiles["bogus2"])
	//require.Equal(t,2,len(cfloglines))
}

type mockS3Client struct {
	listobjectreturn *s3.ListObjectsV2Output
}

func (m *mockS3Client) ListObjectsV2(list *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	listobjectreturn := m.listobjectreturn
	m.listobjectreturn = &s3.ListObjectsV2Output{}
	return listobjectreturn, nil
}

type mockDLMgr struct{}

func (m *mockDLMgr) DownloadWithIterator(ctx aws.Context, iter s3manager.BatchDownloadIterator, opts ...func(*s3manager.Downloader)) (err error) {
	wrbuffers := mockCompressedBuffer()
	var pos int64 = 0
	for _, wrbuffer := range wrbuffers {
		_, err := iter.DownloadObject().Writer.WriteAt(wrbuffer.buffer.Bytes(), pos)
		if err != nil {
			panic(fmt.Sprintf("%v\n", err))
		}
		iter.Next()
		iter.Next()
	}
	return nil
}

func mockCompressedBuffer() (buffer []*wrbuffer) {
	var awsbuf []*aws.WriteAtBuffer
	logtxt := ""
	for i := 1; i < 33; i++ {
		logtxt = logtxt + "\t-"
	}
	logtxt = "-\n-" + logtxt
	for i := 0; i < 2; i++ {
		var buf *bytes.Buffer = &bytes.Buffer{}
		var zw *gzip.Writer = gzip.NewWriter(buf)
		if _, err := zw.Write([]byte(logtxt)); err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		if err := zw.Close(); err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		compressedbuf := aws.NewWriteAtBuffer(buf.Bytes())
		awsbuf = append(awsbuf, compressedbuf)
	}
	buffer = []*wrbuffer{
		{
			filename: "bogus1",
			buffer:   awsbuf[0],
		},
		{
			filename: "bogus2",
			buffer:   awsbuf[1],
		},
	}
	return
}

func mocks3logsclient() (s3logs *S3Logs) {
	// Test with no files to download
	s3client := &mockS3Client{
		listobjectreturn: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("bogus1")},
				{Key: aws.String("bogus2")},
			},
		},
	}
	dlclient := &mockDLMgr{}

	s3logs = &S3Logs{
		s3client:    s3client,
		dlmgr:       dlclient,
		bucket:      "bogus-bucket",
		prefix:      "cf-logs/PREFIX",
		concurrency: 2,
	}
	return
}
