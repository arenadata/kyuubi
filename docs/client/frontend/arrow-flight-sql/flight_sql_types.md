# Flight SQL Type Compatibility Matrix

Flight SQL conversion is engine-agnostic. Engines expose either:

1. Arrow IPC inside a binary thrift column (Spark SQL preferred path); or
2. Ordinary columnar thrift `TRowSet` values converted page-by-page by
   `KyuubiFlightArrowUtils.populateRootFromRowSet`.

## Supported logical types

|   SQL / thrift type   |         Arrow type         |                    Notes                     |
|-----------------------|----------------------------|----------------------------------------------|
| NULL                  | Null                       |                                              |
| BOOLEAN               | Bool                       |                                              |
| TINYINT               | Int(8, signed)             |                                              |
| SMALLINT              | Int(16, signed)            |                                              |
| INT                   | Int(32, signed)            |                                              |
| BIGINT                | Int(64, signed)            |                                              |
| FLOAT                 | FloatingPoint(SINGLE)      |                                              |
| DOUBLE                | FloatingPoint(DOUBLE)      |                                              |
| STRING / VARCHAR      | Utf8                       |                                              |
| BINARY                | Binary                     |                                              |
| DECIMAL(p,s)          | Decimal(p,s)               | Precision/scale required                     |
| DATE                  | Date(DAY)                  |                                              |
| TIMESTAMP             | Timestamp(MICROSECOND, tz) | Time zone required from metadata             |
| complex/non-primitive | unsupported                | Deterministic error; no silent Utf8 fallback |

## Engine status

|    Engine    | Native Arrow IPC | Columnar thrift page conversion |                   Notes                   |
|--------------|------------------|---------------------------------|-------------------------------------------|
| Spark SQL    | Yes              | Yes (fallback)                  | Preferred production path                 |
| Trino        | No               | Yes                             | Uses shared thrift→Arrow page writer      |
| Flink        | No               | Yes                             | Same shared writer                        |
| Hive         | No               | Yes                             | Same shared writer                        |
| JDBC engines | No               | Yes                             | Cursor pages converted one page at a time |

## Conversion rules

- Schema conversion uses `ArrowUtils.toArrowType` for primitives.
- Non-primitive or unmapped types throw `IllegalArgumentException`.
- Value conversion supports bool/int/float/decimal/date/timestamp/binary/utf8.
- Timestamp inputs may be `java.sql.Timestamp`, `Instant`, `LocalDateTime`,
  `OffsetDateTime`, `ZonedDateTime`, epoch numbers, or parseable strings.
- Metadata RPCs use Flight SQL fixed schemas, not query-result schemas.

## Large results

`FlightResultIterator` fetches `kyuubi.frontend.flight.sql.fetch.max.rows` rows
per page, retains only the current page/batch, and emits Arrow RecordBatches
with listener backpressure.
