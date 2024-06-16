#!/bin/bash

if [ $# -ne 1 ]
then
	echo >&2 "Usage: $0 IMAGE_EIF"
	exit 1
fi
image_eif="$1"

if ! command -v nitro-cli &> /dev/null
then
    echo >&2 "nitro-cli not found"
    exit 1
fi

echo "[ec2] starting enclave"
nitro-cli run-enclave \
	--cpu-count 2 \
	--memory 800 \
	--enclave-cid 4 \
	--eif-path "$image_eif" \
	--debug-mode \
	--attach-console
