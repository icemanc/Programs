# Simple Dataflow Java pipeline

This Maven project reads JSON messages from the same Pub/Sub subscription as
`simple_dataflow.py` and appends their `product` and `sales` fields to the
same BigQuery table.

The subscription, table, and default temporary location are set in
`SimpleDataflow.java`. Update them if your Google Cloud resources differ. The
default temporary location must be a writable Cloud Storage path.

Run from this directory, supplying your Dataflow project, region, and a
temporary Cloud Storage path:

```sh
mvn compile exec:java \
  -Dexec.mainClass=com.example.dataflow.SimpleDataflow \
  -Dexec.args="--runner=DataflowRunner --project=YOUR_PROJECT_ID --region=YOUR_REGION --tempLocation=gs://YOUR_BUCKET/temp"
```

Ensure the Pub/Sub subscription and BigQuery dataset exist, and that the
Google Cloud APIs and credentials required to run a Dataflow job are configured.
