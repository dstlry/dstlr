#!/bin/bash

# Setup virtualenv
virtualenv -p /usr/bin/python3 venv && . venv/bin/activate

# Update pip
pip install -U pip

# Install dependencies
pip install -U allennlp jupyter nltk pyspark stanfordnlp spacy toree

# Download spaCy English model
python -m spacy download en

# Install Apache Toree Jupyter kernel (for Spark)
jupyter toree install --user

# Download spark-solr
wget https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/3.6.0/spark-solr-3.6.0-shaded.jar -O spark-solr.jar
