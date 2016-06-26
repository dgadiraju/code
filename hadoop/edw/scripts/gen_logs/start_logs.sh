#!/bin/bash

cd /opt/gen_logs
lib/genhttplogs.py > logs/access.log &
exit 0
