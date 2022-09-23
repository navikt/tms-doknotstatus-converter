#!/bin/env sh
if [ -z "$SERVICEUSER_USERNAME" ]
then
  echo "SERVICEUSER_USERNAME not set, exporting"
  export SERVICEUSER_USERNAME=$(cat /secret/serviceuser/username)
else
  echo "SERVICEUSER_USERNAME already set"
fi

if [ -z "$SERVICEUSER_PASSWORD" ]
then
  echo "SERVICEUSER_PASSWORD not set, exporting"
  export SERVICEUSER_PASSWORD=$(cat /secret/serviceuser/password)
else
  echo "SERVICEUSER_PASSWORD already set"
fi
