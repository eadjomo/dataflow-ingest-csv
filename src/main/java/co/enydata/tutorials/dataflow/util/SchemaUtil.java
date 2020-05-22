package co.enydata.tutorials.dataflow.util;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import com.google.cloud.ByteArray;
import com.google.common.base.Strings;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.schemas.Schema.FieldType.*;


public abstract class SchemaUtil {

    /** Converts Java schema to Beam field. */
    public static Schema.FieldType toFieldType(@Nonnull String type) {

        switch (type.toUpperCase()) {

            case "STRING":
                return STRING;

            case "BYTE":
                return Schema.FieldType.BYTE;

            case "BYTES":
                return Schema.FieldType.BYTES;

            case "SHORT":
                return Schema.FieldType.INT16;

            case "INTEGER":
                return INT64;

            case "INT":
                return Schema.FieldType.INT32;

            case "LONG":
                return INT64;

            case "FLOAT":
                return Schema.FieldType.FLOAT;

            case "DOUBLE":
                return Schema.FieldType.DOUBLE;

            case "BOOLEAN":
                return BOOLEAN;

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






    public static Row castCellValue(Row row, Schema schema)
            throws IllegalArgumentException {
        // The input row's column count could be less than or equal to that of DB schema's.
        if (row.getSchema().getFields().size() > schema.getFields().size()) {
            throw new RuntimeException(
                    String.format(
                            "Parsed row's column count is larger than that of the schema's. "
                                    + "Row size: %d, Column size: %d, Row content: %s",
                            row.getSchema().getFields().size(), schema.getFields().size(), row.getValues().toString()));
        }

        List<Schema.Field> fields = schema.getFields();

        Row.Builder rowBuilder = Row.withSchema(schema);

        // Extract cell by cell
        for (int i = 0; i < fields.size(); i++) {

            Schema.Field field = fields.get(i);
            String cellValue = row.getString(field.getName());
            boolean isNullValue = Strings.isNullOrEmpty(cellValue);
            switch (field.getType().getTypeName()) {
                case BOOLEAN:
                    if (isNullValue) {
                        rowBuilder.addValue(null);
                    } else {
                        Boolean bCellValue;
                        if (cellValue.trim().equalsIgnoreCase("true")) {
                            bCellValue = Boolean.TRUE;
                        } else if (cellValue.trim().equalsIgnoreCase("false")) {
                            bCellValue = Boolean.FALSE;
                        } else {
                            throw new IllegalArgumentException(
                                    cellValue.trim() + " is not recognizable value " + "for BOOL type");
                        }
                        rowBuilder.addValue(Boolean.valueOf(cellValue));
                    }
                    break;
                case INT64:
                    if (isNullValue) {
                        rowBuilder.addValue(null);
                    } else {
                        rowBuilder.addValue(Long.valueOf(cellValue.trim()));
                    }
                    break;
                case FLOAT:
                    if (isNullValue) {
                        rowBuilder.addValue(null);
                    } else {
                        rowBuilder.addValue(Float.valueOf(cellValue.trim()));
                    }
                    break;
                case STRING:
                    rowBuilder.addValue(cellValue);
                    break;
                case DATETIME:
                    if (isNullValue) {
                        rowBuilder.addValue(null);
                    } else {
                        rowBuilder.addValue(org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseDate(cellValue.trim()));

                    }
                    break;

                case BYTES:
                    if (isNullValue) {
                        rowBuilder.addValue(null);
                    } else {
                        rowBuilder.addValue(ByteArray.fromBase64(cellValue.trim()));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unrecognized column data type: " + field.getType());
            }
        }
        return rowBuilder.build();

    }


}
