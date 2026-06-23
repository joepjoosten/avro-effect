# @avro-effect

## 0.0.1

Initial release.

- Add `@avro-effect/schema`, an Effect v4 native Avro schema compiler.
- Add Avro JSON schema to Effect Schema importer.
- Add Avro binary `Schema.Codec<A, Uint8Array>` via `avro(schema)`.
- Support records, enums, arrays, maps, unions, nullable fields, recursive named references, bytes, fixed values, logical type annotations, and tagged record `_tag` omission/restoration.
- Add `@avro-effect/core`, a native Avro runtime foundation with schema model types and binary encode/decode support.
- Wire `@avro-effect/schema` to the native `@avro-effect/core` runtime instead of `avro-js`.
- Add `@avro-effect/schema-registry`, a Confluent Schema Registry client and Avro wire framing package.
- Add `@avro-effect/kafka`, Kafka key/value serializer and deserializer helpers.
- Add `@avro-effect/node`, Node.js Avro object-container file helpers.
- Add package build, tests, and GitHub Actions Changesets release/snapshot workflows.
