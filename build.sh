#!/bin/bash

go build -a -trimpath -ldflags=" -buildid= -s -w -extldflags -w -s -X 'github.com/pika-monitor/pika/pkg/version.Version=0.2.7' -X 'github.com/pika-monitor/pika/pkg/version.AgentVersion=f391cb1'" -o pika .

GOARCH=arm64 go build -a -trimpath -ldflags=" -buildid= -s -w -extldflags -w -s -X 'github.com/pika-monitor/pika/pkg/version.Version=0.2.7' -X 'github.com/pika-monitor/pika/pkg/version.AgentVersion=f391cb1'" -o pika-agent .

exit
