package co.enydata.tutorials.dataflow.reader;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface IReader {

   // public PCollection<TableRow> read(Pipeline p, IngestCSVOptions options);
    public  PCollection<Row> read(Pipeline p, IngestCSVOptions options);
}
