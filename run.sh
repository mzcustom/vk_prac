#!/bin/bash

python server.py --port 8080 &
SERVER_PID=$!

# Ensure the background server is killed when the script exits or is interrupted
trap "kill $SERVER_PID 2>/dev/null" EXIT

sleep 1

go run main.go
