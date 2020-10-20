#!/usr/bin/env bash

# setup virtual environment
python3 -m venv venv && source venv/bin/activate && pip3 install -r local_requirements.txt

# submit spark job
"$SPARK_HOME"/bin/spark-submit \
--master local \
--py-files packages.zip \
etl/src/transform.py

# start Uvicorn server to expose api
uvicorn main:app --reload --app-dir api
