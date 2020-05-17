package co.enydata.tutorials.dataflow.util;

public abstract class FieldType {
    // StringFieldType is a string field type.
   public static final String StringFieldType = "STRING";

    public static final String VARCHARFieldType = "VARCHAR";
    // BytesFieldType is a bytes field type.
    public static final String BytesFieldType  = "BYTES";
    // IntegerFieldType is a integer field type.
    public static final String IntegerFieldType  = "INTEGER";
    // FloatFieldType is a float field type.
    public static final String FloatFieldType  = "FLOAT";
    // BooleanFieldType is a boolean field type.
    public static final String  BooleanFieldType  = "BOOLEAN";
    // TimestampFieldType is a timestamp field type.
    public static final String  TimestampFieldType  = "TIMESTAMP";
    // RecordFieldType is a record field type. It is typically used to create columns with repeated or nested data.
    public static final String RecordFieldType  = "RECORD";
    // DateFieldType is a date field type.
    public static final String DateFieldType  = "DATE";
    // TimeFieldType is a time field type.
    public static final String TimeFieldType  = "TIME";
    // DateTimeFieldType is a datetime field type.
    public static final String DateTimeFieldType  = "DATETIME";
    // NumericFieldType is a numeric field type. Numeric types include integer types, floating point types and the
    // NUMERIC data type.
    public static final String  NumericFieldType  = "NUMERIC";
    // GeographyFieldType is a string field type.  Geography types represent a set of points
    // on the Earth's surface, represented in Well Known Text (WKT) format.
    public static final String  GeographyFieldType  = "GEOGRAPHY";



  public   static String toBigQueryType(String sType) {
        final String fieldType;

        switch (sType.toUpperCase()) {
            case BooleanFieldType:fieldType = BooleanFieldType ;
                break;

            case VARCHARFieldType:fieldType = StringFieldType ;
                break;

            case StringFieldType:fieldType = StringFieldType ;
                break;


            case IntegerFieldType:fieldType = IntegerFieldType ;
                break;

            case FloatFieldType:fieldType = FloatFieldType ;
                break;

            case BytesFieldType:fieldType = BytesFieldType ;
                break;

            case DateFieldType:fieldType = DateFieldType ;
                break;

            case RecordFieldType:fieldType = RecordFieldType ;
                break;

            case TimeFieldType:fieldType = TimeFieldType ;
                break;

            case TimestampFieldType:fieldType = TimestampFieldType ;
                break;

            case NumericFieldType:fieldType = NumericFieldType ;
                break;

            case DateTimeFieldType:fieldType = DateTimeFieldType ;
                break;

            case GeographyFieldType:fieldType = GeographyFieldType ;
                break;

            default:
                throw new IllegalArgumentException("Unknown BigQuey field type " + sType.toUpperCase());
        }
        return fieldType;
    }
}
