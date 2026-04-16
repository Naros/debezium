/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ZonedTimestampType}.
 */
@Tag("UnitTests")
class ZonedTimestampTypeTest {

    @Test
    void testQueryBindingWithNormalTimestampReturnsCastExpression() {
        final var result = ZonedTimestampType.INSTANCE.getQueryBinding(null, null, "2024-01-15T10:30:00Z");
        assertThat(result).isEqualTo("cast(? as timestamptz)");
    }

    @Test
    void testQueryBindingWithNegativeInfinityReturnsCastExpression() {
        final var result = ZonedTimestampType.INSTANCE.getQueryBinding(null, null, "-infinity");
        assertThat(result).isEqualTo("cast(? as timestamptz)");
    }

    @Test
    void testQueryBindingWithPositiveInfinityReturnsCastExpression() {
        final var result = ZonedTimestampType.INSTANCE.getQueryBinding(null, null, "infinity");
        assertThat(result).isEqualTo("cast(? as timestamptz)");
    }

    @Test
    void testQueryBindingWithNullReturnsCastExpression() {
        final var result = ZonedTimestampType.INSTANCE.getQueryBinding(null, null, null);
        assertThat(result).isEqualTo("cast(? as timestamptz)");
    }
}
