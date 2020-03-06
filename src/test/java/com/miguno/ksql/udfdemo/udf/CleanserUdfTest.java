package com.miguno.ksql.udfdemo.udf;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanserUdfTest {

  private static final String OP = "OP";
  private static final String BEFORE = "BEFORE";
  private static final String AFTER = "AFTER";
  private static final String DEBEZIUM = "DEBEZIUM";

  @Test
  public void shouldCleanse() {
    // Given
    CleanserUdf udf = new CleanserUdf();

    Schema beforeSchema = SchemaBuilder.struct().name(BEFORE)
			.field("UID", Schema.STRING_SCHEMA)
			.build();

    Schema afterSchema = SchemaBuilder.struct().name(AFTER)
			.field("UID", Schema.STRING_SCHEMA)
			.build();

    Schema debeziumSchema = SchemaBuilder.struct().name(DEBEZIUM)
			.field("BEFORE", beforeSchema)
			.field("AFTER", afterSchema)
			.field("OP", Schema.STRING_SCHEMA)
			.build();

    Struct beforeStruct = new Struct(beforeSchema)
        .put("UID", "456");

    Struct afterStruct = new Struct(afterSchema)
        .put("UID", "123");

		Struct debeziumStruct = new Struct(debeziumSchema)
      .put("BEFORE", beforeStruct)
      .put("AFTER", afterStruct)
  		.put("OP", "d");

    // When/Then
    assertThat(udf.clean(debeziumStruct)).isEqualTo(debeziumStruct);
  }

}
