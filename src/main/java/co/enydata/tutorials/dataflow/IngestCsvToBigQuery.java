package co.enydata.tutorials.dataflow;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.persister.DefaultPersisterImpl;
import co.enydata.tutorials.dataflow.persister.IPersister;
import co.enydata.tutorials.dataflow.reader.IReader;
import co.enydata.tutorials.dataflow.reader.csv.CsvParser;
import co.enydata.tutorials.dataflow.reader.csv.CsvReaderImpl;
import co.enydata.tutorials.dataflow.transformer.BasicTransformerImpl;
import co.enydata.tutorials.dataflow.transformer.ITransformer;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class IngestCsvToBigQuery {
    private static final Logger logger = LoggerFactory.getLogger(IngestCsvToBigQuery.class);



    public static void main(String[] args) {

        PipelineOptionsFactory.register(IngestCSVOptions.class);
        IngestCSVOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestCSVOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        IReader reader=new CsvReaderImpl();

        PCollection<Row> readerOutPut=reader.read(pipeline,options);

        ITransformer transformer=new BasicTransformerImpl();

        PCollection<TableRow> transformerOutPut=transformer.transform(readerOutPut,TableUtils.getCastExpression(options.getSchema()));

        IPersister persister=new DefaultPersisterImpl();

        persister.persist(transformerOutPut,options);

     /*   pipeline.apply("READ", TextIO.read().from(options.getInputFile()))
                .apply("TRANSFORM", ParDo.of(new CsvParser()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(options.getTableName())
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(TableUtils.getTableSchema(options.getSchema(),options.getHeader(),options.getDelimiter())));

                        */

        pipeline.run().waitUntilFinish();

    }

}
