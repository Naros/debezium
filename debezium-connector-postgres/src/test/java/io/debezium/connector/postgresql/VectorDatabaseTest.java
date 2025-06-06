/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.data.vector.SparseDoubleVector;

public class VectorDatabaseTest {

    @Test
    public void shouldParseSparseVector() {
        final var expectedVector = Map.of((short) 1, 10.0, (short) 11, 20.0, (short) 111, 30.0);
        final var expectedDimensions = (short) 1000;

        var vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10, 11:20, 111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), " {1:10,11:20,111:30}/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30} /1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30}/ 1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30}/1000 ");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

        vector = SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30 }/1000");
        Assertions.assertThat(vector.getInt16("dimensions")).isEqualTo(expectedDimensions);
        Assertions.assertThat(vector.getMap("vector")).isEqualTo(expectedVector);

    }

    @Test
    public void shouldIgnoreErrorInSparseVectorFormat() {
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30}")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30/1000")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "1:10,11:20,111:30}/1000")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:30}1000")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "/1000")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{10,11:20,111:30}/1000")).isNull();
        Assertions.assertThat(SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11#20,111:30}/1000")).isNull();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldFailOnNumberInSparseVectorFormat() {
        SparseDoubleVector.fromLogical(SparseDoubleVector.schema(), "{1:10,11:20,111:x}/1000");
    }
}
