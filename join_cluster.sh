#!/bin/bash

docker exec rabbitmq-local-ha-cluster-rabbitmq2-1 rabbitmqctl stop_app
docker exec rabbitmq-local-ha-cluster-rabbitmq2-1 rabbitmqctl join_cluster rabbitmq1@rabbitmq1
docker exec rabbitmq-local-ha-cluster-rabbitmq2-1 rabbitmqctl start_app

docker exec rabbitmq-local-ha-cluster-rabbitmq3-1 rabbitmqctl stop_app
docker exec rabbitmq-local-ha-cluster-rabbitmq3-1 rabbitmqctl join_cluster rabbitmq1@rabbitmq1
docker exec rabbitmq-local-ha-cluster-rabbitmq3-1 rabbitmqctl start_app