package com.google.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;


public class PubsubToBigQuery {
    
  public static void main(String[] args) {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(<project>);
    options.setTempLocation(<bucket-location>);
    options.setStagingLocation(<bucket-location>);
    options.setRunner(DataflowRunner.class);
    options.setStreaming(true);

    // Topic to pull data from
    String TOPIC_Name = <pubsub-topic>;
    // Big query table location to write to
    String BQ_DS = <BQ-table>;
    
    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
	fields.add(new TableFieldSchema().setName("field1").setType("STRING"));
    fields.add(new TableFieldSchema().setName("field2").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);
    
    Pipeline p = Pipeline.create(options);
    p
    .apply(PubsubIO.readStrings().fromTopic(TOPIC_Name))
    .apply("ConvertDataToTableRows", ParDo.of(new DoFn<String,TableRow>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        System.out.println("Inside processor..");
        String message = c.element();
		System.out.println("Message:" + message);
		String[] lines= message.split(",");
		String field1 = lines[0];
        String field2 = lines[1];
        System.out.println("Creating table row..");
        System.out.println(field1 + " :: " + field2);
        TableRow row = new TableRow()
              .set("field1", field1)
			  .set("field2", field2);
        c.output(row);
      }
    }))
    .apply("InsertTableRowsToBigQuery",
      BigQueryIO.writeTableRows().to(BQ_DS)
      .withSchema(schema)
      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    // Run the pipeline
    p.run().waitUntilFinish();
  }
}