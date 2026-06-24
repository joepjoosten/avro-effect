# @avro-effect/kafka

Kafka serializers and deserializers for the `@avro-effect` packages.

This package is Kafka-client agnostic. It returns functions that work with message buffers, so applications can plug them into KafkaJS, node-rdkafka, or custom Effect Stream consumers.

## Install

```sh
pnpm add @avro-effect/kafka @avro-effect/core @avro-effect/schema-registry effect
```

## Usage

```ts
import { Effect, Layer } from "effect"
import { SchemaRegistry } from "@avro-effect/schema-registry"
import { KafkaAvro, decodeValue, serializeRegistryValue } from "@avro-effect/kafka"

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const KafkaAvroLive = KafkaAvro.layer.pipe(
  Layer.provide(SchemaRegistry.layer({ endpoint: "http://localhost:8081" }))
)

const program = Effect.gen(function*() {
  const value = yield* serializeRegistryValue({ topic: "events", schema }, { id: 1 })

  return yield* decodeValue({
    topic: "events",
    value
  })
}).pipe(
  Effect.provide(KafkaAvroLive)
)
```

## Features

- Effect service and layer API via `KafkaAvro`.
- Plain Avro serializers/deserializers for raw Kafka payloads.
- Confluent Schema Registry backed key/value serializers.
- Confluent frame deserialization by embedded schema id.
- Topic, record, and topic-record subject naming strategies.
- Contextual decode errors that include topic, partition, offset, and key/value location.
- Lower-level helpers stay Kafka-client agnostic and accept a `SchemaRegistryClient` directly.
