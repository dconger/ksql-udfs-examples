package com.miguno.ksql.udfdemo.udaf;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.confluent.ksql.function.udaf.TableUdaf;

import static org.assertj.core.api.Assertions.assertThat;

public class SumUdafTest {

  @Test
  public void shouldSumBigInts() {
    // Given
    TableUdaf<Long, Long, Long> udaf = SumUdaf.createSumBigInt();

    // When/Then
    assertThat(udaf.initialize()).isEqualTo(0L);

    assertThat(udaf.undo(4L, 10L)).isEqualTo(6L);
    assertThat(udaf.undo(-4L, 10L)).isEqualTo(14L);
    assertThat(udaf.undo(null, 10L)).isEqualTo(10L);
    assertThat(udaf.undo(4L, 0L)).isEqualTo(-4L);
    assertThat(udaf.undo(0L, 0L)).isEqualTo(0L);

    assertThat(udaf.aggregate(4L, 10L)).isEqualTo(14L);
    assertThat(udaf.aggregate(4L, -10L)).isEqualTo(-6L);
    assertThat(udaf.aggregate(null, 10L)).isEqualTo(10L);
    assertThat(udaf.aggregate(-4L, 10L)).isEqualTo(6L);
    assertThat(udaf.aggregate(-4L, -10L)).isEqualTo(-14L);

    assertThat(udaf.merge(10L, 30L)).isEqualTo(40L);
    assertThat(udaf.merge(10L, -30L)).isEqualTo(-20L);
    assertThat(udaf.merge(-10L, 30L)).isEqualTo(20L);
    assertThat(udaf.merge(-10L, -30L)).isEqualTo(-40L);
  }
}
