package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(name = "sum_null", description = "sums numbers, treats null as 0 (zero)")
public class SumUdaf {

  @UdafFactory(description = "sums BIGINT numbers")
  public static TableUdaf<Long, Long, Long> createSumBigInt() {
    return new TableUdaf<Long, Long, Long>() {
      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long undo(final Long valueToUndo, final Long aggregate) {
        long v = valueToUndo == null? 0: valueToUndo;
        return aggregate - v;
      }

      @Override
      public Long aggregate(final Long value, final Long aggregate) {
        long v = value == null? 0: value;
        return aggregate + v;
      }

      @Override
      public Long map(final Long aggregate) {
        return aggregate;
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }
    };
  }
}
