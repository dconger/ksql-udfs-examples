package com.miguno.ksql.udfdemo.udaf;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import io.confluent.ksql.function.udaf.TableUdaf;

import static org.assertj.core.api.Assertions.assertThat;

public class ListSumUdafTest {

  @Test
  public void shouldSumLongs() {
    // Given
    TableUdaf<List<Long>, Long> udaf = ListSumUdaf.sumLongList();

    // When/Then
    assertThat(udaf.initialize()).isEqualTo(0L);

    List<Long> ll = new ArrayList<Long>();

    ll.add(0, 4L);
    ll.add(1, 3L);

    assertThat(udaf.aggregate(ll, 10L)).isEqualTo(17L);
  }
}
