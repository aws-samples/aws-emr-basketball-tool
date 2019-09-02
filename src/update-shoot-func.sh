#!/bin/bash

FUNCTION=shoot_func
mkdir $FUNCTION
virtualenv --python=python3.7 venv

source venv/bin/activate
pip install awscli

cp venv/bin/aws $FUNCTION/
cp -r venv/lib/python3.7/site-packages/* $FUNCTION/
cp shoot.py $FUNCTION/

cd $FUNCTION/

# in OSX
# sed -i '' "1s/.*/\#\!\/var\/lang\/bin\/python/" aws
# in EC2 linux
sed -i "1s/.*/\#\!\/var\/lang\/bin\/python/" aws

zip -r9 function.zip *
aws lambda update-function-code --function-name ${1} --zip-file fileb://function.zip
