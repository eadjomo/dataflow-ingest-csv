package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicTransformerImpl implements ITransformer {

    private static final Logger logger = LoggerFactory.getLogger(BasicTransformerImpl.class);

    @Override
    public  PCollection<Row> transform(PCollection<Row> rowPCollection,
                                            SchemaDataInfo schemaDataInfo) {

        return BasicTransformer.transform(rowPCollection,schemaDataInfo);

    }
}
