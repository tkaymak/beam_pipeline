import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import formats.Click;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BeamTest {
    private static final Logger LOG = LoggerFactory.getLogger(BeamTest.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().configure(
            DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
    // FORMAT: project-id:dataset-id.tablename
    // CHANGEME
    private static final String tableName = "project-id:dataset-id.tablename";

    /**
     * Deserialize the JSON logline
     */
    static class DeserializeLogLineIntoClick extends DoFn<String, Click> {

        private static final DateTimeFormatter dateTimeFormat =
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");


        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String clickLogLine = c.element();
            try {
                Click click = objectMapper.readValue(clickLogLine, Click.class);
                click.ts = extractInstantFromTimestamp(click.timestamp);
                c.outputWithTimestamp(click, click.ts);
            } catch (Exception e) {
                LOG.error("Unable to parse line: " + c.element().toString());
            }
        }

        /**
         * Parses an Instant from a given timestamp
         *
         * @param timestamp the timestamp to parse as string
         * @return the Instant representing the parsed timestamp
         * @throws IllegalArgumentException if nothing can be parsed
         */
        public Instant extractInstantFromTimestamp(String timestamp) throws Exception {
            try {
                return new Instant(dateTimeFormat.parseMillis(timestamp));
            } catch (IllegalArgumentException e) {
                LOG.warn(String.format("Timestamp \"%s\" does not match any configured timestamp pattern!" +
                                " Line ignored.",
                        timestamp));
                throw new IllegalArgumentException();
            }
        }
    }

    /**
     * Convert each Click object to a TableRow
     */
    private static class BuildRowFn extends DoFn<Click, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toTableRow());
        }
    }

    public static void main(String[] args) throws IOException {

        TableSchema outputSchema = new TableSchema().setFields(Click.getTableFieldSchema());

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create();

        Pipeline p = Pipeline.create(options);
        p.apply("Read logfile ", TextIO.read().from("gs://tamedia_tx_conf/clicks.2015-12-01.log"))
                .apply("Parse JSON", ParDo.of(new DeserializeLogLineIntoClick()))
                .apply("Convert to TableRow", ParDo.of(new BuildRowFn()))
                .apply(
                        "Write to BigQuery",
                        BigQueryIO.writeTableRows().to(tableName)
                                .withSchema(outputSchema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run();

    }
}
