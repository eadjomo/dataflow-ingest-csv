package co.enydata.tutorials.dataflow.common;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface IngestCSVOptions extends PipelineOptions {

    @Description("Input file path")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Header")
    @Validation.Required
    String getHeader();
    void setHeader(String value);

    @Description("delimiter")
    @Validation.Required
    String getDelimiter();
    void setDelimiter(String value);

    @Description("table schema in format Json format")
    String getSchema();
    void setSchema(String value);

    @Description("BigQuery Table in format PROJECTID:DATASET.TABLEANAME")
    @Validation.Required
    String getTableName();
    void setTableName(String value);
}
