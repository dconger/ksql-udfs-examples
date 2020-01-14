package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "my_average", description = "Computes the average.")
public class AverageUdaf {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";

  @UdafFactory(description = "Compute average of column with type Long.",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  // Can be used with table aggregations
  public static TableUdaf<Long, Struct, Double> averageLong() {

    final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new TableUdaf<Long, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_LONG).put(SUM, 0L).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Long newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);
      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_LONG)
            .put(SUM, agg1.getInt64(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Long valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  @UdafFactory(description = "Compute average of column with type Integer.",
      aggregateSchema = "STRUCT<SUM integer, COUNT bigint>")
  public static TableUdaf<Integer, Struct, Double> averageInt() {

    final Schema STRUCT_INT = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT32_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new TableUdaf<Integer, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_INT).put(SUM, 0).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Integer newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_INT)
            .put(SUM, aggregate.getInt32(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_INT)
            .put(SUM, agg1.getInt32(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Integer valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_INT)
            .put(SUM, aggregate.getInt32(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  @UdafFactory(description = "Compute average of column with type Double.",
      aggregateSchema = "STRUCT<SUM double, COUNT bigint>")
  public static TableUdaf<Double, Struct, Double> averageDouble() {

    final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
        .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    return new TableUdaf<Double, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_DOUBLE).put(SUM, 0.0).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Double newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_DOUBLE)
            .put(SUM, aggregate.getFloat64(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getFloat64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_DOUBLE)
            .put(SUM, agg1.getFloat64(SUM) + agg2.getFloat64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Double valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_DOUBLE)
            .put(SUM, aggregate.getFloat64(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  // This method shows providing an initial value to an aggregated, i.e., it would be called
  // with my_average(col1, 'some_initial_value')
  @UdafFactory(description = "Compute average of length of strings",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  public static Udaf<String, Struct, Double> averageStringLength(final String initialString) {

    final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new Udaf<String, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_LONG).put(SUM, (long) initialString.length()).put(COUNT, 1L);
      }

      @Override
      public Struct aggregate(final String newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) + newValue.length())
            .put(COUNT, aggregate.getInt64(COUNT) + 1);
      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_LONG)
            .put(SUM, agg1.getInt64(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }
    };
  }
}
