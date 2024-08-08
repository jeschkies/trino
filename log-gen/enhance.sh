#!/bin/bash
while IFS= read j; do
    jq -c --arg id "$(shuf -n 1 /etc/tenants)" '."user-identifier"= $id' <<< "$j"
done
