# Apache Beam example for Tamedia TX 2017

This repository contains a very simple example pipeline that is using Apache Beam to read a logfile from Google Cloud Storage (GCS) and write it to Google BigQuery (BQ)

It can be run locally and using the Google Cloud Dataflow Service.

### Prerequisites:
To run it successfully there needs to be a bucket in Google Cloud Storage to store temporary files as well as a dataset/table to write the results to.
