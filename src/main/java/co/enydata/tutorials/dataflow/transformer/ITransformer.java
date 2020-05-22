package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface ITransformer {
    //public  PCollection<TableRow> transform(PCollection<TableRow> rowPCollection,List<String>fieldList);
    public  PCollection<Row> transform(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo);
}
