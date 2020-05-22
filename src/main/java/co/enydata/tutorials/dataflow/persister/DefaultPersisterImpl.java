package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class DefaultPersisterImpl implements IPersister{
    @Override
    public WriteResult persist(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo) {
        return BigQueryPersister.persist(rowPCollection,schemaDataInfo);
    }
}
