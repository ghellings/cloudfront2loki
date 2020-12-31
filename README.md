# cloudfront2loki

## Description
This is for consuming Cloudfront logs from s3 and pushing them to Loki.

## Usage

This assumes you've either set AWS key ENVs or you're using roles. You'll need to generate a configfile that looks like the following.

```
region: "us-east-1"
bucket: "YOURBUCKETNAME"
prefix: "SUBDIRECTORY/CLOUDFRONT_DISTRO_ID"
concurrency: 2
lokihost: "LOKI-HOSTNAME:3100"
lokilabels: "{source=\"cloudfront\",job=\"cloudfront2loki\"}"
lokibatchsize: 10
lokibatchwaitseconds: 5
lokiloglevel: DISABLE
startafterfile: "SUBDIRECTORY/CLOUDFRONT_FILE_YOU_WANT_TO_START_FROM"
loopsleepseconds: 60
loglevel: info
# ignorelokilatestlog: true
```

## Behavior

This will start up and query Loki using the labels from the config.  It will find the last log entry in Loki, get the filename from that and start downloading from s3 all the files from that hour onward.  You can change this behavior with the ```ignorelokilatestlog``` option and it will instead just start with the ```startafterfile``` alphanumerically (**NOT BY DATE**).  It does avoid re-importing logs into Loki it's imported before by checking every time it sees a new filename if Loki already has any entries for that filename and skips the file if it does.  This means if it stops in the middle of importing a file, you'll need to remove all of those entries from Loki to get it to re-import the file.

## Docker

TODO
