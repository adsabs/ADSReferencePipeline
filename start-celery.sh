#!/bin/bash

hostname=`hostname`

celery worker --pidfile=logs/celery1.pid -A adsrefpipe.tasks -Q task_process_reference -c 1 -n celery1@$hostname &
