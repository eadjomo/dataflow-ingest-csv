package co.enydata.tutorials.dataflow;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.persister.DefaultPersisterImpl;
import co.enydata.tutorials.dataflow.persister.IPersister;
import co.enydata.tutorials.dataflow.reader.IReader;
import co.enydata.tutorials.dataflow.reader.csv.CsvParser;
import co.enydata.tutorials.dataflow.reader.csv.CsvReaderImpl;
import co.enydata.tutorials.dataflow.transformer.BasicTransformerImpl;
import co.enydata.tutorials.dataflow.transformer.ITransformer;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class IngestCsvToBigQuery {
    private static final Logger logger = LoggerFactory.getLogger(IngestCsvToBigQuery.class);



    public static void main(String[] args) throws Exception {

        PipelineOptionsFactory.register(IngestCSVOptions.class);

        IngestCSVOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(IngestCSVOptions.class);

        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");

        Pipeline pipeline = Pipeline.create(options);
        ObjectMapper mapper = new ObjectMapper();

        BlobId blobId = BlobId.of(options.getSchema(),"schema2.json");
        byte[] content = StorageOptions.getDefaultInstance().getService().readAllBytes(blobId);
        String contentString = new String(content, UTF_8);

        SchemaDataInfo schemaDataInfo=mapper.readValue(contentString,SchemaDataInfo.class);

        IReader reader=new CsvReaderImpl();

        PCollection<Row> readerOutPut=reader.read(pipeline,schemaDataInfo,options);

        ITransformer transformer=new BasicTransformerImpl();

        PCollection<Row> transformerOutPut=transformer.transform(readerOutPut,schemaDataInfo);

        IPersister persister=new DefaultPersisterImpl();

        persister.persist(transformerOutPut,schemaDataInfo);

        pipeline.run().waitUntilFinish();

    }

}
