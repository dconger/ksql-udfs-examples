package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

@UdafDescription(name = "collect", description = "collect")
public class ArthurUdaf {

    @UdafFactory(description = "collect")
    public static TableUdaf<Long, List<Long>, Long> create() {

        return new TableUdaf<Long, List<Long>, Long>() {

            @Override
            public List<Long> initialize() {
                return new ArrayList<>();
            }

            @Override
            public List<Long> aggregate(final Long newValue, final List<Long> aggregate) {
                aggregate.add(newValue);

                return aggregate;
            }

            @Override
            public Long map(final List<Long> aggregate) {
              return sumList(aggregate);
            }

            @Override
            public List<Long> merge(final List<Long> aggOne, final List<Long> aggTwo) {
                List<Long> list = new ArrayList<>();
                list.addAll(aggOne);
                list.addAll(aggTwo);
                return list;
            }

            @Override
            public List<Long> undo(Long value, List<Long> aggregate) {
                aggregate.remove(value);
                return aggregate;
            }

            private long sumList(final List<Long> list) {
              return sum(list, 0L, (a,b) -> a + b);
            }
        };
    }

    private static <T> T sum(
        final Iterable<T> list,
        final T initial,
        final BinaryOperator<T> summer) {

      T sum = initial;
      for (final T v: list) {
        if (v == null) {
          continue;
        }
        sum = summer.apply(sum, v);
      }
      return sum;
    }
}
