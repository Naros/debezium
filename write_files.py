summary = (
    "Fixes `ZonedTimestampType.getQueryBinding()` to always return `cast(? as timestamptz)` "
    "so that infinity timestamp values work correctly in multi-record JDBC batches.\n\n"
    "- `ZonedTimestampType.getQueryBinding()`: Removed value-dependent branching; the method now "
    'unconditionally returns `"cast(? as timestamptz)"`. Previously it returned a plain `?` for '
    "non-infinity values, but the SQL template is built from the first record only and reused for "
    "all records in a JDBC batch. When the first record had a regular timestamp, subsequent records "
    "with `-infinity` or `infinity` failed because the bound `Types.VARCHAR` value could not be "
    "implicitly coerced into a `timestamptz` column without an explicit cast.\n\n"
    "- `JdbcFieldDescriptor.getQueryBinding()`: Removed the lazily-initialized `queryBinding` cache "
    "field. The method now delegates directly to `jdbcType.getQueryBinding(column, schema, value)` "
    "on every call. This restores true immutability (consistent with the `@Immutable` annotation) "
    "and prevents value-dependent bindings from being locked in on the first call.\n\n"
    "- `JdbcSinkInsertModeIT.testInsertModeInfinityValueAsNonFirstRecordInBatch()`: New integration "
    "test parameterized over both standard INSERT and UNNEST batch modes. It consumes a two-record "
    "batch where the first record holds a normal timestamp and the second holds `-infinity`, then "
    "asserts both rows are present in the target table.\n\n"
    "## Notes for reviewer\n\n"
    "The `cast(? as timestamptz)` binding works for both regular timestamps (bound as "
    "`Types.TIMESTAMP_WITH_TIMEZONE` / `OffsetDateTime`) and infinity values (bound as "
    '`Types.VARCHAR` / `"-infinity"` / `"infinity"`). PostgreSQL accepts the explicit cast for '
    "both cases, so changing from a conditional to an unconditional cast is safe.\n\n"
    "The `@Immutable` annotation on `JdbcFieldDescriptor` was already violated by the mutable "
    "`queryBinding` cache; removing the field makes the annotation accurate again.\n"
)

with open('/tmp/agent-summary.md', 'w') as f:
    f.write(summary)

with open('/tmp/commit-subject.md', 'w') as f:
    f.write('Fix infinity timestamptz binding for non-first records in batch\n')

print('Both files written successfully.')
