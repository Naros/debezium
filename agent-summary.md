Fixes `ZonedTimestampType.getQueryBinding()` to always return `cast(? as timestamptz)` so that infinity timestamp values work correctly in multi-record JDBC batches.

- `ZonedTimestampType.getQueryBinding()`: Removed value-dependent branching; the method now unconditionally returns `"cast(? as timestamptz)"`. Previously it returned a plain `?` for non-infinity values, but the SQL template is built from the first record only and reused for all records in a JDBC batch. When the first record had a regular timestamp, subsequent records with `-infinity` or `infinity` failed because the bound `Types.VARCHAR` value could not be implicitly coerced into a `timestamptz` column without an explicit cast.

- `JdbcFieldDescriptor.getQueryBinding()`: Removed the lazily-initialized `queryBinding` cache field. The method now delegates directly to `jdbcType.getQueryBinding(column, schema, value)` on every call. This restores true immutability (consistent with the `@Immutable` annotation) and prevents value-dependent bindings from being locked in on the first call.

- `JdbcSinkInsertModeIT.testInsertModeInfinityValueAsNonFirstRecordInBatch()`: New integration test parameterized over both standard INSERT and UNNEST batch modes. It consumes a two-record batch where the first record holds a normal timestamp and the second holds `-infinity`, then asserts both rows are present in the target table.

## Notes for reviewer

The `cast(? as timestamptz)` binding works for both regular timestamps (bound as `Types.TIMESTAMP_WITH_TIMEZONE` / `OffsetDateTime`) and infinity values (bound as `Types.VARCHAR` / `"-infinity"` / `"infinity"`). PostgreSQL accepts the explicit cast for both cases, so changing from a conditional to an unconditional cast is safe.

The `@Immutable` annotation on `JdbcFieldDescriptor` was already violated by the mutable `queryBinding` cache; removing the field makes the annotation accurate again.
