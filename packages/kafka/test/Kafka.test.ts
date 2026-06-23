import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { makeClient } from "@avro-effect/schema-registry"
import {
  avroDeserializer,
  avroSerializer,
  decodeMessageValue,
  registryValueSerializer
} from "../src/index.js"

describe("@avro-effect/kafka", () => {
  const schema = {
    type: "record",
    name: "Event",
    fields: [{ name: "id", type: "long" }]
  } as const

  it("serializes plain Avro payloads", () => {
    const serialize = avroSerializer(schema)
    const deserialize = avroDeserializer(schema)

    expect(deserialize(serialize({ id: 1 }))).toEqual({ id: 1 })
  })

  it.effect("serializes and deserializes registry framed values", () =>
    Effect.gen(function*() {
      const requests: Array<{ readonly method: string; readonly path: string }> = []
      const client = makeClient({
        endpoint: "http://registry.test",
        fetch: async (input, init) => {
          const url = new URL(String(input))
          requests.push({ method: init?.method ?? "GET", path: url.pathname })
          if (url.pathname === "/subjects/events-value/versions") {
            return json({ id: 3, subject: "events-value", version: 1 })
          }
          if (url.pathname === "/schemas/ids/3") {
            return json({ schema: JSON.stringify(schema) })
          }
          return new Response("not found", { status: 404 })
        }
      })
      const serialize = registryValueSerializer(client, { topic: "events", schema })
      const payload = yield* serialize({ id: 2 })
      const decoded = yield* decodeMessageValue(client, {
        topic: "events",
        partition: 0,
        offset: "1",
        value: payload
      })

      expect(decoded).toEqual({ id: 2 })
      expect(requests).toEqual([
        { method: "POST", path: "/subjects/events-value/versions" }
      ])
    }))
})

const json = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" }
  })
