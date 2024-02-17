#!/bin/bash -x
export FLASK_ENV="testing"
echo $FLASK_ENV
python3 ServiceDB/app.py > outputDB.txt 2>&1 &
python3 ServiceS3/app.py > outputS3.txt 2>&1 &
python3 Gateway/app.py


