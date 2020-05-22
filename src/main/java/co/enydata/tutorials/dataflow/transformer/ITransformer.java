package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public interface ITransformer {
    //public  PCollection<TableRow> transform(PCollection<TableRow> rowPCollection,List<String>fieldList);
    public  PCollection<Row> transform(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo);
}
