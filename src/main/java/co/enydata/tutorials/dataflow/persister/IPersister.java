package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface IPersister  {

    public WriteResult persist(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo);
}
