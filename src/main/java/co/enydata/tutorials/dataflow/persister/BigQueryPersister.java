package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class BigQueryPersister {

    public static WriteResult persist(PCollection<TableRow> tableRowPCollection, IngestCSVOptions options) {
        return  tableRowPCollection.apply("WRITE", BigQueryIO.writeTableRows()
                .to(options.getTableName())
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND)
                .withSchema(TableUtils.getTableSchema(options.getSchema(),options.getHeader(),options.getDelimiter())));
    }
}
