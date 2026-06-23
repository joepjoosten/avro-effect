# @avro-effect/schema

## 0.0.1

Initial release.

- Add Effect v4 native Avro schema compiler.
- Add Avro JSON schema to Effect Schema importer.
- Add Avro binary `Schema.Codec<A, Uint8Array>` via `avro(schema)` using `@avro-effect/core`.
- Support records, enums, arrays, maps, unions, nullable fields, recursive named references, bytes, fixed values, logical type annotations, and tagged record `_tag` omission/restoration.
