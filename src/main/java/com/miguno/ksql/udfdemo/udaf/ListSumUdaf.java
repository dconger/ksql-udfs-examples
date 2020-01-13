package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import java.util.function.BinaryOperator;

@UdafDescription(name = "sum_list", description = "Returns the sum of elements contained in the list.")
public class ListSumUdaf {

  @UdafFactory(description = "sum long values in a list into a single long")
  public static TableUdaf<List<Long>, Long> sumLongList() {
    return new TableUdaf<List<Long>, Long>() {

      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long aggregate(final List<Long> valueToAdd, final Long aggregateValue) {
        if (valueToAdd == null) {
          return aggregateValue;
        }
        return aggregateValue + sumList(valueToAdd);
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }

      @Override
      public Long undo(final List<Long> valueToUndo, final Long aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return aggregateValue - sumList(valueToUndo);
      }

      private long sumList(final List<Long> list) {
        return sum(list, initialize(), (a,b) -> a + b);
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
