package com.example.dataflow;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SimpleDataflow {
    private static final String SUBSCRIPTION =
            "projects/upbeat-math-480123-r1/subscriptions/sales-events-sub";
    private static final String BIGQUERY_TABLE =
            "upbeat-math-480123-r1:sales_dataset.streaming_sales";
    private static final String DEFAULT_TEMP_LOCATION = "gs://dataflow_demo_bucket/temp";

    public static void main(String[] args) {
        DataflowPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(DataflowPipelineOptions.class);
        options.setStreaming(true);
        if (options.getTempLocation() == null) {
            options.setTempLocation(DEFAULT_TEMP_LOCATION);
        }

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Read from Pub/Sub", PubsubIO.readStrings().fromSubscription(SUBSCRIPTION))
                .apply(
                        "Parse JSON",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                .via(SimpleDataflow::parseMessage))
                .apply(
                        "Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .to(BIGQUERY_TABLE)
                                .withSchema(createSchema())
                                .withCreateDisposition(
                                        BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(
                                        BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));
        pipeline.run();
    }

    private static TableRow parseMessage(String message) {
        JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        TableRow row = new TableRow();
        row.set("product", json.get("product").getAsString());
        row.set("sales", json.get("sales").getAsDouble());
        return row;
    }

    private static TableSchema createSchema() {
        return new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema().setName("product").setType("STRING"),
                                new TableFieldSchema().setName("sales").setType("FLOAT")));
    }
}
