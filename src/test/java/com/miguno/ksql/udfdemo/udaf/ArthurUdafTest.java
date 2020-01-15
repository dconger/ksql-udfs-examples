package com.miguno.ksql.udfdemo.udaf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import io.confluent.ksql.function.udaf.TableUdaf;
import org.junit.Test;

public class ArthurUdafTest {

  @Test
  public void shouldSumLongs() {
    final TableUdaf<Long, List<Long>, Long> udaf = ArthurUdaf.create();
    List<Long> agg = udaf.initialize();
    final long[] values = new long[] {1L, 1L, 1L, 1L, 1L};
    for (final long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final long result = udaf.map(agg);

    assertThat(result, equalTo(5L));
  }
}
