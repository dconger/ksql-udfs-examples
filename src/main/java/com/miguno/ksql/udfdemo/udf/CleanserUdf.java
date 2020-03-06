package com.miguno.ksql.udfdemo.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 *
 * select CLEANSER_UDF( ??? ) as RESULT FROM SOME_STREAM EMIT CHANGES;
 *
 */
@UdfDescription(name = "cleanser_udf", description = "cleanses payload", author = "Dan Conger", version = "1.0.0" )
public class CleanserUdf {
    private static final String OP = "OP";
    private static final String BEFORE = "BEFORE";
    private static final String AFTER = "AFTER";
    private static final String DEBEZIUM = "DEBEZIUM";

    private static final Schema beforeSchema = SchemaBuilder.struct().name(BEFORE)
      .field("UID", Schema.STRING_SCHEMA)
      .build();

    private static final Schema afterSchema = SchemaBuilder.struct().name(AFTER)
      .field("UID", Schema.STRING_SCHEMA)
      .build();

    private static final Schema debeziumSchema = SchemaBuilder.struct().name(DEBEZIUM)
      .field(BEFORE, beforeSchema)
      .field(AFTER, afterSchema)
      .field(OP, Schema.STRING_SCHEMA)
      .build();

    @Udf(
      schema = "STRUCT <BEFORE STRUCT<uid VARCHAR>, AFTER STRUCT<uid VARCHAR>, OP VARCHAR>",
      description = "Clean the payload"
    )
    public Struct clean(
        @UdfParameter(schema = "STRUCT <BEFORE STRUCT<uid VARCHAR>, AFTER STRUCT<uid VARCHAR>, OP VARCHAR>") final Struct PAYLOAD
    )
        return new Struct(debeziumSchema)
                .put(BEFORE, PAYLOAD.getStruct("BEFORE"))
                .put(AFTER, PAYLOAD.getStruct("AFTER"))
                .put(OP, PAYLOAD.getString("OP"));
    }
}
