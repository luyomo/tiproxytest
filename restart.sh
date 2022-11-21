#!/bin/bash

echo "Hello world"

sleep 5
tiup cluster restart mgtest -y --node 182.83.2.92:4000
sleep 10
tiup cluster restart mgtest -y --node 182.83.1.182:4000
