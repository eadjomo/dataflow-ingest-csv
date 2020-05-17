package co.enydata.tutorials.dataflow.reader.csv;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.reader.IReader;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

public class CsvReaderImpl implements IReader {
 /*   @Override
    public PCollection<TableRow> read(Pipeline p, IngestCSVOptions options) {
        PCollection<String> input = p.apply("READ INPUT", TextIO.read().from(options.getInputFile()));
        return new CsvParser().parse(input);


    }*/


    @Override
    public  PCollection<Row> read(Pipeline p, IngestCSVOptions options) {
        PCollection<String> input = p.apply("READ INPUT", TextIO.read().from(options.getInputFile()));
        return CsvParser.parse(input).setRowSchema(TableUtils.getSchema(options.getHeader(),options.getDelimiter()));


    }
}
