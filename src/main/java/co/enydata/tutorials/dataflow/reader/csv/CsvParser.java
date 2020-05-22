package co.enydata.tutorials.dataflow.reader.csv;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

public class CsvParser {

    public static PCollection<Row> parse(PCollection<FileIO.ReadableFile> input, Schema schema){
       IngestCSVOptions options=input.getPipeline().getOptions().as(IngestCSVOptions.class);
       String header=options.getHeader();
       String delimiter=options.getDelimiter();

        return input.apply("PARSE INPUT", ParDo.of(new DoFn<FileIO.ReadableFile, CSVRecord >(){
                    @ProcessElement
                    public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> receiver) throws IOException {
                        InputStream is = Channels.newInputStream(element.open());
                        Reader reader = new InputStreamReader(is);
                        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                                .withHeader(header.split(delimiter))
                                .withDelimiter(delimiter.charAt(0))
                                .withFirstRecordAsHeader()
                                .withIgnoreEmptyLines()
                                .parse(reader);
                        for (CSVRecord record : records) { receiver.output(record); }
                    }

                })).apply("CsvRecord to Beam Row",ParDo.of(new DoFn<CSVRecord, Row>() {
            @ProcessElement
            public void process(@Element CSVRecord record, OutputReceiver<Row> receiver){
               Row.Builder rowBuilder=Row.withSchema(schema);

                for (String s : record) {
                    rowBuilder.addValue(s);
                }
                receiver.output(rowBuilder.build());
            }
        })).setRowSchema(schema);
    }


}
