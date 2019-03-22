#!/bin/bash

###
# Configuration
###

# The path for conda (given at installation time)
CONDA_PATH="/opt/miniconda3/etc/profile.d/conda.sh"

# The spark-solr version to download
SPARK_SOLR_VERSION=3.6.0

###
# Setup
###

# Source the conda path
source $CONDA_PATH

# Create and activate conda env
conda create -n dstlr python=3.7 && conda activate dstlr

# Install dependencies
pip install -U allennlp jupyter pyspark spacy toree

# See https://github.com/jupyter/notebook/issues/2664
pip install "tornado<6"

# Download spaCy English model
python -m spacy download en

# Download spark-solr
wget "https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/$SPARK_SOLR_VERSION/spark-solr-$SPARK_SOLR_VERSION-shaded.jar" -O spark-solr.jar