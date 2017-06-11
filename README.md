# Apache Beam Example for Tamedia TX 2017

This repository contains a very simple example pipeline that is using Apache Beam to read a logfile from Google Cloud Storage (GCS) and write it to Google BigQuery (BQ)

It can be run locally and using the Google Cloud Dataflow Service.

### Prerequisites:
To run it successfully there needs to be a bucket in Google Cloud Storage to store temporary files as well as a dataset/table to write the results to (change `tableName` in line 29). The system that is executing the pipeline should have Java 8 installed.

### How to build it:
`mvn package` should do the trick, if everything compiles you can find `logfiles-bundled-0.1-SNAPSHOT.jar` in the `target` directory.

### How to run it:
- Locally:
`java target/logfiles-bundled-0.1-SNAPSHOT.jar --tempLocation=gs://lcl_dataflow/dataflow --project=plucky-aegis-94413`

- Remotely (on Google Cloud Dataflow):
`java -jar target/logfiles-bundled-0.1-SNAPSHOT.jar --tempLocation=gs://lcl_dataflow/dataflow --runner=DataflowRunner --project=plucky-aegis-94413 --maxNumWorkers=1`
