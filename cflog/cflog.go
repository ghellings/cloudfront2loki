package cflog

type CFLog struct {
	Filename                    string
	Date                        string
	Time                        string
	X_edge_location             string
	Sc_bytes                    string
	C_ip                        string
	Cs_method                   string
	Cs_Host                     string
	Cs_uri_stem                 string
	Sc_status                   string
	Cs_Referer                  string
	Cs_User_Agent               string
	Cs_uri_query                string
	Cs_Cookie                   string
	X_edge_result_type          string
	X_edge_request_id           string
	X_host_header               string
	Cs_protocol                 string
	Cs_bytes                    string
	Time_taken                  string
	X_forwarded_for             string
	Ssl_protocol                string
	Ssl_cipher                  string
	X_edge_response_result_type string
	Cs_protocol_version         string
	Fle_status                  string
	Fle_encrypted_fields        string
	C_port                      string
	Time_to_first_byte          string
	X_edge_detailed_result_type string
	Sc_content_type             string
	Sc_content_len              string
	Sc_range_start              string
	Sc_range_end                string
}

func MockCFLog(filename string, response_type string, date string, time string) (log *CFLog) {
	log = &CFLog{
		Filename:                    filename,
		Date:                        date,
		Time:                        time,
		X_edge_location:             "-",
		Sc_bytes:                    "-",
		C_ip:                        "-",
		Cs_method:                   "-",
		Cs_Host:                     "-",
		Cs_uri_stem:                 "-",
		Sc_status:                   "-",
		Cs_Referer:                  "-",
		Cs_User_Agent:               "-",
		Cs_uri_query:                "-",
		Cs_Cookie:                   "-",
		X_edge_result_type:          "-",
		X_edge_request_id:           "-",
		X_host_header:               "-",
		Cs_protocol:                 "-",
		Cs_bytes:                    "-",
		Time_taken:                  "-",
		X_forwarded_for:             "-",
		Ssl_protocol:                "-",
		Ssl_cipher:                  "-",
		X_edge_response_result_type: "-",
		Cs_protocol_version:         "-",
		Fle_status:                  "-",
		Fle_encrypted_fields:        "-",
		C_port:                      "-",
		Time_to_first_byte:          "-",
		X_edge_detailed_result_type: response_type,
		Sc_content_type:             "-",
		Sc_content_len:              "-",
		Sc_range_start:              "-",
		Sc_range_end:                "-",
	}
	return
}
