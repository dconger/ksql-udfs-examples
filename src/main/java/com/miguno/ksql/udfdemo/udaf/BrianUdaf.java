package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@UdafDescription(name = "brian", description = "brian")
public class BrianUdaf {

    private static final String OP = "OP";
    private static final String BEFORE = "BEFORE";
    private static final String AFTER = "AFTER";

    @UdafFactory(
      paramSchema = "STRUCT <BEFORE STRUCT<uid VARCHAR>, AFTER STRUCT<uid VARCHAR>, OP VARCHAR>",
      description = "brian"
    )
    public static TableUdaf<Struct, List<Struct>, Long> create() {

        return new TableUdaf<Struct, List<Struct>, Long>() {

            @Override
            public List<Struct> initialize() {
                return new ArrayList<>();
            }

            @Override
            public List<Struct> aggregate(final Struct newValue, final List<Struct> aggregate) {
                // newValue.getString(VALUE)
                aggregate.add(newValue.getStruct(AFTER));

                return aggregate;
            }

            @Override
            public Long map(final List<Struct> aggregate) {
              return 0L;
            }

            @Override
            public List<Struct> merge(final List<Struct> aggOne, final List<Struct> aggTwo) {
                List<Struct> list = new ArrayList<>();
                list.addAll(aggOne);
                list.addAll(aggTwo);
                return list;
            }

            @Override
            public List<Struct> undo(Struct value, List<Struct> aggregate) {
                aggregate.remove(value);
                return aggregate;
            }
        };
    }
}
