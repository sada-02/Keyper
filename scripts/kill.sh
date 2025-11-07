#!/bin/bash
for port in {8080..8084}; do
    pid=$(lsof -t -i TCP:$port)

    if [ -n "$pid" ]; then
        echo "🛑 Port $port in use by PID $pid — killing..."
        kill -9 "$pid"
    else
        echo "✅ Port $port is free"
    fi
done

for port in 7000 7001; do
    pid=$(lsof -t -i TCP:$port)

    if [ -n "$pid" ]; then
        echo "🛑 Port $port in use by PID $pid — killing..."
        kill -9 "$pid"
    else
        echo "✅ Port $port is free"
    fi
done

for port in {12000...12003}; do
    pid=$(lsof -t -i TCP:$port)

    if [ -n "$pid" ]; then
        echo "🛑 Port $port in use by PID $pid — killing..."
        kill -9 "$pid"
    else
        echo "✅ Port $port is free"
    fi
done

for port in {11000..11003}; do
    pid=$(lsof -t -i TCP:$port)

    if [ -n "$pid" ]; then
        echo "🛑 Port $port in use by PID $pid — killing..."
        kill -9 "$pid"
    else
        echo "✅ Port $port is free"
    fi
done
