package co.enydata.tutorials.dataflow.reader.csv;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.reader.IReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.stream.Collectors;

public class CsvReaderImpl implements IReader {
  /*@Override
    public PCollection<TableRow> read(Pipeline p, IngestCSVOptions options) {
        PCollection<String> input = p.apply("READ INPUT", TextIO.read().from(options.getInputFile()));
        return CsvParser.parse(input);


    }*/


    @Override
    public  PCollection<Row> read(Pipeline p, SchemaDataInfo schemaDataInfo,IngestCSVOptions options) {


        Schema schema= Schema.builder().addFields(schemaDataInfo.getFields().stream()
                .map(s -> Schema.Field.of(s.getName(), Schema.FieldType.STRING))
                .collect(Collectors.toList())).build();

        String file=options.getInputFile();

        PCollection<FileIO.ReadableFile> input =  p.apply(FileIO.match().filepattern(file))
                .apply("READ INPUT",FileIO.readMatches());

        return CsvParser.parse(input,schema);
    }
}
