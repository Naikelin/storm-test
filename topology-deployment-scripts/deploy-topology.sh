#!/bin/bash
echo "Waiting for storm nimbus..."
/topology-deployment-scripts/wait-for.sh nimbus:6627 -- echo "Nimbus is up"


#/apache-storm-*/bin/storm jar /topology/storm-word-count.jar io.koszolko.storm.example.WordCountTopology word-count-topology
storm jar /topology/storm-starter-2.4.0.jar org.apache.storm.starter.RollingTopWords topology
