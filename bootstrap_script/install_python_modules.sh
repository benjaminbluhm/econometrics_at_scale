#!/bin/bash -xe

# Install Python modules:
sudo pip install -U \
  pandas            \
  scipy				\
  fastparquet		\
  s3fs				\
  s3io				\
  joblib	\
  statsmodels