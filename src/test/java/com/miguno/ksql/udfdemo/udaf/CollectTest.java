package com.miguno.ksql.udfdemo.udaf;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.confluent.ksql.function.udaf.TableUdaf;

import static org.assertj.core.api.Assertions.assertThat;

public class CollectTest {

  @Test
  public void shouldCollectValues() {
    // Given
    TableUdaf<String, List<String>> udaf = Collect.create();

    List<String> agg = new ArrayList<String>();
    // When/Then
    assertThat(udaf.initialize()).isEqualTo(agg);

    agg.add(0, "arthur");
    agg.add(1, "dan");

    List<String> expected = new ArrayList<String>(Arrays.asList("arthur", "dan", "brian"));

    assertThat(udaf.aggregate("brian", agg)).isEqualTo(expected);
  }
}
