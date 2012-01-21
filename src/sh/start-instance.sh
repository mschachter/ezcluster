#!/bin/sh

export EC2_HOME=/usr/bin
export EC2_PRIVATE_KEY=/tmp/private_key.pem
export EC2_CERT=/tmp/cert.pem
export EC2_URL=#REGION_URL#
export AWS_ACCESS_KEY_ID=#ACCESS_KEY#
export AWS_SECRET_ACCESS_KEY=#SECRET_KEY#

#APPLICATION_SPECIFIC_SCRIPT#

#copy ez_cluster from S3

#run ez_cluster Daemon


