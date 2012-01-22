#!/bin/sh

export EC2_HOME=/usr/bin
export EC2_PRIVATE_KEY=/tmp/private_key.pem
export EC2_CERT=/tmp/cert.pem
export EC2_URL=#REGION_URL#
export EC2_INSTANCE_ID=#INSTANCE_ID#
export EC2_DNS_NAME=#DNS_NAME#
export AWS_ACCESS_KEY_ID=#ACCESS_KEY#
export AWS_SECRET_ACCESS_KEY=#SECRET_KEY#
export NUM_JOBS_PER_INSTANCE=#NUM_JOBS_PER_INSTANCE#

export PYTHONPATH=$PYTHONPATH:/tmp/ezcluster/src/python

#copy ezcluster from S3
cd /tmp
s3cmd --config /tmp/s3cfg get s3://#BUCKET#/ezcluster.tgz
tar xvzf ezcluster.tgz
rm ezcluster.tgz

#APPLICATION_SCRIPT#

#run ezcluster Daemon


