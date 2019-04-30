#!/bin/bash -xe

# Repeatedly remove older numpy version from cluster (otherwise fastparquet import will fail)
sudo pip install -U numpy
sudo pip install -U numpy

# Install Python modules:
sudo pip install -U \
  numpy             \
  pandas            \
  scipy				\
  fastparquet		\
  s3fs				\
  s3io				\
  joblib	\
  findspark \
  statsmodels