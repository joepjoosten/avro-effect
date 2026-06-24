# @avro-effect/schema

Effect Schema <-> Avro schema compiler and binary codec for Effect v4.

This package connects Effect Schema v4 with the native Avro runtime from `@avro-effect/core`.

## Install

```sh
pnpm add @avro-effect/schema effect
```

## Usage

```ts
import { Schema } from "effect"
import { avro, fromAvroSchema, Long, toAvroSchema } from "@avro-effect/schema"

class User extends Schema.Class<User>("User")({
  id: Long,
  name: Schema.String
}) {}

const avroJson = toAvroSchema(User)

const UserAvro = avro(User)
const encode = Schema.encodeSync(UserAvro)
const decode = Schema.decodeUnknownSync(UserAvro)

const buffer = encode(new User({ id: 1, name: "Ada" }))
const user = decode(buffer)

const Imported = fromAvroSchema(avroJson)
```

## Features

- Compile Effect Schema v4 ASTs to Avro JSON schemas
- Build Effect schemas from Avro JSON schemas
- Re-export schema-backed Avro model definitions from `@avro-effect/core`
- Produce a `Schema.Codec<A, Uint8Array>` for Avro binary payloads
- Support records, enums, arrays, maps, unions, nullable fields, recursive references, bytes, fixed values, and logical type annotations
- Omit tagged class `_tag` fields from Avro records while restoring them after decoding
- Schema-backed internal conversion errors for consistent Effect error reporting
