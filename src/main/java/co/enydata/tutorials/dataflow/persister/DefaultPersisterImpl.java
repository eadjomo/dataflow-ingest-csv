package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class DefaultPersisterImpl implements IPersister{
    @Override
    public WriteResult persist(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo) {
        return BigQueryPersister.persist(rowPCollection,schemaDataInfo);
    }
}
