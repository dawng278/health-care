#!/bin/bash

# Kiểm tra nếu chưa format NameNode
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

echo "Starting NameNode..."
hdfs namenode
