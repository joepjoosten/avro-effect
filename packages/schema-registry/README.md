# @avro-effect/schema-registry

Effect-friendly Confluent Schema Registry client and Avro wire framing.

## Install

```sh
pnpm add @avro-effect/schema-registry @avro-effect/core effect
```

## Usage

```ts
import { makeClient, encodeWithRegistry, decodeWithRegistry } from "@avro-effect/schema-registry"

const client = makeClient({
  endpoint: "http://localhost:8081"
})

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const encoded = encodeWithRegistry(client, {
  subject: "events-value",
  schema,
  value: { id: 1 }
})

const decoded = decodeWithRegistry(client, encoded)
```

## Features

- Register and lookup Avro schemas by subject.
- Fetch schemas by id, subject version, and latest version.
- Confluent wire frame helpers: magic byte `0`, 4-byte schema id, Avro payload.
- Subject naming strategies: topic, record, and topic-record.
- Basic auth and bearer token support.
- In-memory schema id and subject/schema caches.
