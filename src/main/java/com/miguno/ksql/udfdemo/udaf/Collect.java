package com.miguno.ksql.udfdemo.udaf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@UdafDescription(name = "collect", description = "collect")
public class Collect {

    @UdafFactory(description = "collect")
    public static TableUdaf<String, List<String>, List<String>> create() {

        return new TableUdaf<String, List<String>, List<String>>() {

            @Override
            public List<String> initialize() {
                return new ArrayList<>();
            }

            @Override
            public List<String> aggregate(final String newValue, final List<String> aggregate) {
                if (!aggregate.contains(newValue)) {
                    aggregate.add(newValue);
                }
                return aggregate;
            }

            @Override
            public List<String> map(final List<String> aggregate) {
              return aggregate;
            }

            @Override
            public List<String> merge(final List<String> aggOne, final List<String> aggTwo) {
                List<String> list = new ArrayList<>();
                list.addAll(aggOne);
                list.addAll(aggTwo);
                return list;
            }

            @Override
            public List<String> undo(String value, List<String> aggregate) {
                aggregate.remove(value);
                return aggregate;
            }
        };
    }
}
