# @avro-effect/node

Node.js Avro object container file helpers for `@avro-effect/core`.

File helpers depend on the Effect v4 `FileSystem.FileSystem` service. Provide the Node implementation layer in applications; tests can provide a `FileSystem.layerNoop` override.

## Install

```sh
pnpm add @avro-effect/node @avro-effect/core effect
```

## Usage

```ts
import { decodeContainer, encodeContainer, writeContainerFile } from "@avro-effect/node"

const schema = {
  type: "record",
  name: "Event",
  fields: [{ name: "id", type: "long" }]
} as const

const file = encodeContainer(schema, [{ id: 1 }, { id: 2 }])
const decoded = decodeContainer(file)
```

## Features

- Avro object container file header, metadata, sync marker, and block handling.
- `null` and raw `deflate` codecs.
- File helpers built on the Effect `FileSystem` service.
- Async iterable reader for Node streams.
