package s3logs

import (
	// "fmt"
	"bytes"
	"encoding/csv"
	"compress/gzip"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type dlmgrinterface interface {
	DownloadWithIterator(aws.Context, s3manager.BatchDownloadIterator, ...func(*s3manager.Downloader)) error
}
type s3interface interface {
	ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

type S3Logs struct {
	dlmgr							dlmgrinterface
	s3client                        s3interface
	bucket  					    string
	prefix                          string
	startafter                      string
	dlconcurrency					int
	concurrency						int
}

type CFLog struct {
	date 							string
	time   							string
	x_edge_location   				string
	sc_bytes   						string
	c_ip   							string
	cs_method   					string
	cs_Host   						string
	cs_uri_stem   					string
	sc_status   					string
	cs_Referer   					string
	cs_User_Agent   				string
	cs_uri_query   					string
	cs_Cookie   					string
	x_edge_result_type   			string
	x_edge_request_id   			string
	x_host_header   				string
	cs_protocol   					string
	cs_bytes   						string
	time_taken   					string
	x_forwarded_for   				string
	ssl_protocol   					string
	ssl_cipher   					string
	x_edge_response_result_type   	string
	cs_protocol_version   			string
	fle_status   					string
	fle_encrypted_fields  			string
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

func New(region string, bucket string, prefix string, concurrency int) (s3logs *S3Logs) {
	s3logs = &S3Logs{
		s3client: GetS3client(region),
		dlmgr:	  GetDlmgr(region),
	}
	return
}

func (s *S3Logs) getListofFiles(startafter string) (files []*string, nextfile string, err error) { 
	if s.dlconcurrency < 1 || s.dlconcurrency > s.concurrency {
		s.dlconcurrency = s.concurrency
	}
	keys,err := s.s3client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: 	aws.String(s.bucket),
		Prefix: 	aws.String(s.prefix),
		StartAfter: aws.String(startafter),
		MaxKeys:	aws.Int64(int64(s.dlconcurrency)+1),
	})
	if err != nil {
	    return
	}
	for _,item := range keys.Contents {
		files = append(files,item.Key)
	}
	if len(files) == s.concurrency+1 {
		nextfile = *files[s.concurrency]
		files = files[0:s.concurrency-1]
	}
	return
}

func (s *S3Logs) parseCFLogs(buffers []*aws.WriteAtBuffer) (cfloglines []*CFLog, err error) {
	for _,buff := range buffers {
		gr, err := gzip.NewReader( bytes.NewReader(buff.Bytes()) )
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		reader := csv.NewReader(gr)
		reader.LazyQuotes = true
		reader.Comma = '\t'
		reader.Read()
		reader.Read()
		reader.FieldsPerRecord = 26
		rows, err := reader.ReadAll()
		if err != nil {
			return nil, err
		}
		for _,fields := range rows {
			cflogline := &CFLog{
				date:							fields[0],
				time:   						fields[1],
				x_edge_location:   				fields[2],
				sc_bytes:   					fields[3],
				c_ip:  							fields[4],
				cs_method:   					fields[5],
				cs_Host:   						fields[6],
				cs_uri_stem:   					fields[7],
				sc_status:   					fields[8],
				cs_Referer:   					fields[9],
				cs_User_Agent:   				fields[10],
				cs_uri_query:  					fields[11],
				cs_Cookie:   					fields[12],
				x_edge_result_type:   			fields[13],
				x_edge_request_id:   			fields[14],
				x_host_header:   				fields[15],
				cs_protocol:   					fields[16],
				cs_bytes:   					fields[17],
				time_taken:   					fields[18],
				x_forwarded_for:   				fields[19],
				ssl_protocol:  					fields[20],
				ssl_cipher:   					fields[21],
				x_edge_response_result_type:   	fields[22],
				cs_protocol_version:   			fields[23],
				fle_status:   					fields[24],
				fle_encrypted_fields:  			fields[25],
			}
			cfloglines = append(cfloglines,cflogline)
		}
	}
	return
}

func (s *S3Logs) Download() (cfloglines []*CFLog, nextstartfile string, err error ) {
	nextfile := s.startafter
	filecount := 0
	for {
		files := []*string{}
		files,nextfile,err = s.getListofFiles(nextfile)
		if err != nil {
			return nil,nextfile,err
		}
		objects := []s3manager.BatchDownloadObject{}
		buffers := []*aws.WriteAtBuffer{}
		for _, filename := range files {
			buffer := aws.NewWriteAtBuffer([]byte{})
			obj := s3manager.BatchDownloadObject{
				Object: &s3.GetObjectInput {
					Bucket: aws.String(s.bucket),
					Key: filename,
				},
				Writer: buffer,
			}
			objects = append(objects,obj)
			buffers = append(buffers,buffer)
		}
		iter := &s3manager.DownloadObjectsIterator{Objects: objects}
		if err := s.dlmgr.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
			return nil,nextfile,err
		}
		cfloglines_add,err := s.parseCFLogs(buffers)
		if err != nil {
			return nil,nextfile,err
		}
		cfloglines = append(cfloglines,cfloglines_add...)
		filecount = filecount+1
		if nextfile == "" || filecount >= s.concurrency {
			break
		}
	}
	nextstartfile = nextfile
    return
}