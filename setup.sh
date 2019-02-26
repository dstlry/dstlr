#!/bin/bash

###
# Configuration
###

SPARK_SOLR_VERSION=3.6.0

###
# Setup
###

# Setup virtualenv
virtualenv -p /usr/bin/python3 venv && . venv/bin/activate

# Update pip
pip install -U pip

# Install dependencies
pip install -U allennlp jupyter pyspark stanfordnlp spacy toree

# Download spaCy English model
python -m spacy download en

# Download spark-solr
wget "https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/$SPARK_SOLR_VERSION/spark-solr-$SPARK_SOLR_VERSION-shaded.jar" -O spark-solr.jar