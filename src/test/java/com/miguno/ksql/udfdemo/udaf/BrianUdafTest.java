package com.miguno.ksql.udfdemo.udaf;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.confluent.ksql.function.udaf.TableUdaf;

import static org.assertj.core.api.Assertions.assertThat;

public class BrianUdafTest {

  private static final String DEBEZIUM = "DEBEZIUM";
  private static final String BEFORE = "BEFORE";
  private static final String AFTER = "AFTER";

  @Test
  public void shouldBrianUdafValues() {

    // Given
    TableUdaf<Struct, List<Struct>, Long> udaf = BrianUdaf.create();

    List<Struct> agg = new ArrayList<Struct>();

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

		Struct debeziumStruct = new Struct(debeziumSchema);

		// debeziumStruct.put("OP", "d");
  }
}
