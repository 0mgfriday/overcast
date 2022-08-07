#!/bin/bash

for name in $(aws lambda list-functions --query 'Functions[].FunctionName' --output text); do
    location=$(aws lambda get-function --function-name $name --query 'Code.Location' --output text)
    wget -O "${name}.zip" $location
done