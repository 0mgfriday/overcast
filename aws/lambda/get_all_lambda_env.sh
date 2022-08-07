#!/bin/bash
# Returns null if env not set

for name in $(aws lambda list-functions --query 'Functions[].FunctionName' --output text); do
    echo "${name}: "
    aws lambda get-function-configuration --function-name $name --query 'Environment'
done