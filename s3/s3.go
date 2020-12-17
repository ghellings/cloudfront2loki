package s3logs

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ghellings/cloudfront2loki/cflog"
)

type dlmgrinterface interface {
	DownloadWithIterator(aws.Context, s3manager.BatchDownloadIterator, ...func(*s3manager.Downloader)) error
}
type s3interface interface {
	ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

type S3Logs struct {
	dlmgr         dlmgrinterface
	s3client      s3interface
	bucket        string
	prefix        string
	startafter    string
	dlconcurrency int
	concurrency   int
}

type wrbuffer struct {
	filename string
	buffer   *aws.WriteAtBuffer
}

func GetDlmgr(region string) (downloader dlmgrinterface) {
	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(region)))
	downloader = s3manager.NewDownloader(sess)
	return
}

func GetS3client(region string) (s3client s3interface) {
	sess := session.Must(session.NewSession())
	s3client = s3.New(sess, aws.NewConfig().WithRegion(region))
	return
}

func New(region string, bucket string, prefix string, concurrency string) (s3logs *S3Logs) {
	con, err := strconv.Atoi(concurrency)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	s3logs = &S3Logs{
		bucket:      bucket,
		prefix:      prefix,
		concurrency: con,
		s3client:    GetS3client(region),
		dlmgr:       GetDlmgr(region),
	}
	return
}

func (s *S3Logs) getListofFiles(startafter string) (files []*string, nextfile string, err error) {
	if s.dlconcurrency < 1 || s.dlconcurrency > s.concurrency {
		s.dlconcurrency = s.concurrency
	}
	keys, err := s.s3client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:     aws.String(s.bucket),
		Prefix:     aws.String(s.prefix),
		StartAfter: aws.String(startafter),
		MaxKeys:    aws.Int64(int64(s.dlconcurrency)),
	})
	if err != nil {
		return
	}
	for _, item := range keys.Contents {
		files = append(files, item.Key)
	}
	if len(files) == s.concurrency {
		nextfile = *files[len(files)-1]
	}
	return
}

func (s *S3Logs) parseCFLogs(buffers []*wrbuffer) (cfloglines []*cflog.CFLog, err error) {
	for _, wrbuff := range buffers {
		gr, err := gzip.NewReader(bytes.NewReader(wrbuff.buffer.Bytes()))
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		reader := csv.NewReader(gr)
		reader.LazyQuotes = true
		reader.Comma = '\t'
		reader.Read()
		reader.Read()
		reader.FieldsPerRecord = 33
		rows, err := reader.ReadAll()
		if err != nil {
			return nil, err
		}
		for _, fields := range rows {
			cflogline := &cflog.CFLog{
				Filename:                    wrbuff.filename,
				Date:                        fields[0],
				Time:                        fields[1],
				X_edge_location:             fields[2],
				Sc_bytes:                    fields[3],
				C_ip:                        fields[4],
				Cs_method:                   fields[5],
				Cs_Host:                     fields[6],
				Cs_uri_stem:                 fields[7],
				Sc_status:                   fields[8],
				Cs_Referer:                  fields[9],
				Cs_User_Agent:               fields[10],
				Cs_uri_query:                fields[11],
				Cs_Cookie:                   fields[12],
				X_edge_result_type:          fields[13],
				X_edge_request_id:           fields[14],
				X_host_header:               fields[15],
				Cs_protocol:                 fields[16],
				Cs_bytes:                    fields[17],
				Time_taken:                  fields[18],
				X_forwarded_for:             fields[19],
				Ssl_protocol:                fields[20],
				Ssl_cipher:                  fields[21],
				X_edge_response_result_type: fields[22],
				Cs_protocol_version:         fields[23],
				Fle_status:                  fields[24],
				Fle_encrypted_fields:        fields[25],
				C_port:                      fields[26],
				Time_to_first_byte:          fields[27],
				X_edge_detailed_result_type: fields[28],
				Sc_content_type:             fields[29],
				Sc_content_len:              fields[30],
				Sc_range_start:              fields[31],
				Sc_range_end:                fields[32],
			}
			cfloglines = append(cfloglines, cflogline)
		}
	}
	return
}

func (s *S3Logs) Download(startafterfile string) (cfloglines []*cflog.CFLog, nextstartafterfile string, err error) {
	filecount := 0
	nextstartafterfile = startafterfile
	for {
		files := []*string{}
		files, nextstartafterfile, err = s.getListofFiles(nextstartafterfile)
		if err != nil {
			return nil, nextstartafterfile, err
		}
		objects := []s3manager.BatchDownloadObject{}
		buffers := []*wrbuffer{}
		for _, filename := range files {
			buffer := aws.NewWriteAtBuffer([]byte{})
			obj := s3manager.BatchDownloadObject{
				Object: &s3.GetObjectInput{
					Bucket: aws.String(s.bucket),
					Key:    filename,
				},
				Writer: buffer,
			}
			objects = append(objects, obj)
			buffers = append(buffers, &wrbuffer{
				filename: *filename,
				buffer:   buffer,
			})
		}
		iter := &s3manager.DownloadObjectsIterator{Objects: objects}
		if err := s.dlmgr.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
			return nil, nextstartafterfile, err
		}
		cfloglines_add, err := s.parseCFLogs(buffers)
		if err != nil {
			return nil, nextstartafterfile, err
		}
		cfloglines = append(cfloglines, cfloglines_add...)
		filecount = filecount + 1
		if nextstartafterfile == "" || filecount >= s.concurrency {
			break
		}
	}
	return
}
