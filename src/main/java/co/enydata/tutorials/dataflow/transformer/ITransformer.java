package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public interface ITransformer {
    public  PCollection<TableRow> transform(PCollection<Row> rowPCollection,List<String>fieldList);
}
