package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;

public interface IPersister  {

    public WriteResult persist(PCollection<TableRow> tableRowPCollection, IngestCSVOptions options);
}
