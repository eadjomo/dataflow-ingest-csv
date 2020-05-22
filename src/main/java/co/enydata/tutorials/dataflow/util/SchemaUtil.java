package co.enydata.tutorials.dataflow.util;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import org.apache.beam.sdk.schemas.Schema;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public abstract class SchemaUtil {

    /** Converts Java schema to Beam field. */
    public static Schema.FieldType toFieldType(@Nonnull String type) {

        switch (type.toUpperCase()) {

            case "STRING":
                return Schema.FieldType.STRING;

            case "BYTE":
                return Schema.FieldType.BYTE;

            case "BYTES":
                return Schema.FieldType.BYTES;

            case "SHORT":
                return Schema.FieldType.INT16;

            case "INTEGER":
                return Schema.FieldType.INT64;

            case "INT":
                return Schema.FieldType.INT32;

            case "LONG":
                return Schema.FieldType.INT64;

            case "FLOAT":
                return Schema.FieldType.FLOAT;

            case "DOUBLE":
                return Schema.FieldType.DOUBLE;

            case "BOOLEAN":
                return Schema.FieldType.BOOLEAN;

            case "DECIMAL":
                return Schema.FieldType.DECIMAL;

            case "DATE":
                return Schema.FieldType.DATETIME;

            case "DATETIME":
                return Schema.FieldType.DATETIME;

            case "TIMESTAMP":
                return Schema.FieldType.DATETIME;
            default:
                throw new IllegalArgumentException("Unknown Beam fieldType " + type.toUpperCase());
        }
    }





    public static Schema getSchema(SchemaDataInfo schemaDataInfo,boolean withComputedField){

        List<Schema.Field> fields=schemaDataInfo.getFields().stream()
                .map(f->Schema.Field.of(f.getName(),toFieldType(f.getType())))
                .collect(Collectors.toList());

        if(withComputedField)  schemaDataInfo.getComputedfields()
                .forEach(f -> fields.add(Schema.Field
                        .of(f.getName(),toFieldType(f.getType()))));

        return Schema.builder().addFields(fields).build();
    }


}
