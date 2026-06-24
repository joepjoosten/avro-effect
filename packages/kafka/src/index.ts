import * as Avro from "@avro-effect/core"
import * as Registry from "@avro-effect/schema-registry"
import { Context, Effect, Layer } from "effect"

export interface KafkaMessage {
  readonly topic: string
  readonly partition?: number
  readonly offset?: string | number
  readonly key?: Uint8Array | null
  readonly value?: Uint8Array | null
  readonly headers?: Record<string, Uint8Array | string | undefined>
}

export type KafkaPayloadLocation = "key" | "value"

export class KafkaAvroError extends Error {
  readonly topic: string | undefined
  readonly partition: number | undefined
  readonly offset: string | number | undefined
  readonly location: KafkaPayloadLocation | undefined
  readonly cause: unknown

  constructor(message: string, context: KafkaErrorContext = {}) {
    super(message)
    this.name = "KafkaAvroError"
    this.topic = context.topic
    this.partition = context.partition
    this.offset = context.offset
    this.location = context.location
    this.cause = context.cause
  }
}

export interface KafkaErrorContext {
  readonly topic?: string
  readonly partition?: number
  readonly offset?: string | number
  readonly location?: KafkaPayloadLocation
  readonly cause?: unknown
}

export interface AvroSerializerOptions {
  readonly parseOptions?: Avro.ParseOptions
}

export const avroSerializer = <A>(
  schema: Avro.AvroSchema,
  options: AvroSerializerOptions = {}
) => (value: A): Uint8Array =>
  Avro.encode(schema, value, options.parseOptions)

export const avroDeserializer = <A = unknown>(
  schema: Avro.AvroSchema,
  options: AvroSerializerOptions = {}
) => (payload: Uint8Array): A =>
  Avro.decode<A>(schema, payload, options.parseOptions)

export interface RegistrySerializerOptions extends Omit<Registry.RegisterSchemaRequest, "subject"> {
  readonly subject?: string
  readonly topic?: string
  readonly isKey?: boolean
  readonly subjectNameStrategy?: Registry.SubjectNameStrategy
  readonly autoRegister?: boolean
  readonly parseOptions?: Avro.ParseOptions
}

export const registrySerializer = <A>(
  client: Registry.SchemaRegistryClient,
  options: RegistrySerializerOptions
) => (value: A): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError> =>
  Effect.gen(function*() {
    const subject = yield* Effect.try({
      try: () => resolveSubject(options),
      catch: (error) => error instanceof KafkaAvroError
        ? error
        : new KafkaAvroError(message(error), { cause: error })
    })
    return yield* Registry.encodeWithRegistry(client, {
      subject,
      schema: options.schema,
      value,
      ...(options.schemaType === undefined ? {} : { schemaType: options.schemaType }),
      ...(options.references === undefined ? {} : { references: options.references }),
      ...(options.autoRegister === undefined ? {} : { autoRegister: options.autoRegister }),
      ...(options.parseOptions === undefined ? {} : { parseOptions: options.parseOptions })
    })
  })

export const registryKeySerializer = <A>(
  client: Registry.SchemaRegistryClient,
  options: Omit<RegistrySerializerOptions, "isKey">
) => registrySerializer<A>(client, { ...options, isKey: true })

export const registryValueSerializer = <A>(
  client: Registry.SchemaRegistryClient,
  options: Omit<RegistrySerializerOptions, "isKey">
) => registrySerializer<A>(client, { ...options, isKey: false })

export const registryDeserializer = <A = unknown>(
  client: Registry.SchemaRegistryClient,
  options?: Avro.ParseOptions
) => (payload: Uint8Array): Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError> =>
  Registry.decodeWithRegistry<A>(client, payload, options)

export const registryKeyDeserializer = registryDeserializer
export const registryValueDeserializer = registryDeserializer

export const decodeMessagePayload = <A = unknown>(
  client: Registry.SchemaRegistryClient,
  message: KafkaMessage,
  location: KafkaPayloadLocation,
  options?: Avro.ParseOptions
): Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError> =>
  Effect.gen(function*() {
    const payload = location === "key" ? message.key : message.value
    if (payload === null || payload === undefined) {
      return yield* Effect.fail(new KafkaAvroError(`Kafka message ${location} is empty`, messageContext(message, location)))
    }
    return yield* Registry.decodeWithRegistry<A>(client, payload, options)
  })

export const decodeMessageKey = <A = unknown>(
  client: Registry.SchemaRegistryClient,
  message: KafkaMessage,
  options?: Avro.ParseOptions
) => decodeMessagePayload<A>(client, message, "key", options)

export const decodeMessageValue = <A = unknown>(
  client: Registry.SchemaRegistryClient,
  message: KafkaMessage,
  options?: Avro.ParseOptions
) => decodeMessagePayload<A>(client, message, "value", options)

export const encodeMessagePayload = <A>(
  client: Registry.SchemaRegistryClient,
  message: Pick<KafkaMessage, "topic">,
  location: KafkaPayloadLocation,
  schema: Avro.AvroSchema,
  value: A,
  options: Omit<RegistrySerializerOptions, "topic" | "schema" | "isKey"> = {}
) =>
  registrySerializer<A>(client, {
    ...options,
    topic: message.topic,
    schema,
    isKey: location === "key"
  })(value)

export interface KafkaAvroService {
  readonly registrySerializer: <A>(
    options: RegistrySerializerOptions
  ) => (value: A) => Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly registryKeySerializer: <A>(
    options: Omit<RegistrySerializerOptions, "isKey">
  ) => (value: A) => Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly registryValueSerializer: <A>(
    options: Omit<RegistrySerializerOptions, "isKey">
  ) => (value: A) => Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly registryDeserializer: <A = unknown>(
    options?: Avro.ParseOptions
  ) => (payload: Uint8Array) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError>
  readonly registryKeyDeserializer: <A = unknown>(
    options?: Avro.ParseOptions
  ) => (payload: Uint8Array) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError>
  readonly registryValueDeserializer: <A = unknown>(
    options?: Avro.ParseOptions
  ) => (payload: Uint8Array) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError>
  readonly decodeMessagePayload: <A = unknown>(
    message: KafkaMessage,
    location: KafkaPayloadLocation,
    options?: Avro.ParseOptions
  ) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly decodeMessageKey: <A = unknown>(
    message: KafkaMessage,
    options?: Avro.ParseOptions
  ) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly decodeMessageValue: <A = unknown>(
    message: KafkaMessage,
    options?: Avro.ParseOptions
  ) => Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
  readonly encodeMessagePayload: <A>(
    message: Pick<KafkaMessage, "topic">,
    location: KafkaPayloadLocation,
    schema: Avro.AvroSchema,
    value: A,
    options?: Omit<RegistrySerializerOptions, "topic" | "schema" | "isKey">
  ) => Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError>
}

export class KafkaAvro extends Context.Service<KafkaAvro, KafkaAvroService>()(
  "@avro-effect/kafka/KafkaAvro"
) {
  static readonly layer: Layer.Layer<KafkaAvro, never, Registry.SchemaRegistry> = Layer.effect(
    KafkaAvro,
    Effect.gen(function*() {
      const registry = yield* Registry.SchemaRegistry

      return KafkaAvro.of({
        registrySerializer: <A>(options: RegistrySerializerOptions) => registrySerializer<A>(registry, options),
        registryKeySerializer: <A>(options: Omit<RegistrySerializerOptions, "isKey">) =>
          registryKeySerializer<A>(registry, options),
        registryValueSerializer: <A>(options: Omit<RegistrySerializerOptions, "isKey">) =>
          registryValueSerializer<A>(registry, options),
        registryDeserializer: <A = unknown>(options?: Avro.ParseOptions) =>
          registryDeserializer<A>(registry, options),
        registryKeyDeserializer: <A = unknown>(options?: Avro.ParseOptions) =>
          registryKeyDeserializer<A>(registry, options),
        registryValueDeserializer: <A = unknown>(options?: Avro.ParseOptions) =>
          registryValueDeserializer<A>(registry, options),
        decodeMessagePayload: <A = unknown>(
          message: KafkaMessage,
          location: KafkaPayloadLocation,
          options?: Avro.ParseOptions
        ) =>
          decodeMessagePayload<A>(registry, message, location, options),
        decodeMessageKey: <A = unknown>(message: KafkaMessage, options?: Avro.ParseOptions) =>
          decodeMessageKey<A>(registry, message, options),
        decodeMessageValue: <A = unknown>(message: KafkaMessage, options?: Avro.ParseOptions) =>
          decodeMessageValue<A>(registry, message, options),
        encodeMessagePayload: <A>(
          message: Pick<KafkaMessage, "topic">,
          location: KafkaPayloadLocation,
          schema: Avro.AvroSchema,
          value: A,
          options: Omit<RegistrySerializerOptions, "topic" | "schema" | "isKey"> = {}
        ) =>
          encodeMessagePayload<A>(registry, message, location, schema, value, options)
      })
    })
  )
}

export const serializeRegistry = <A>(
  options: RegistrySerializerOptions,
  value: A
): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.registrySerializer<A>(options)(value))

export const serializeRegistryKey = <A>(
  options: Omit<RegistrySerializerOptions, "isKey">,
  value: A
): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.registryKeySerializer<A>(options)(value))

export const serializeRegistryValue = <A>(
  options: Omit<RegistrySerializerOptions, "isKey">,
  value: A
): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.registryValueSerializer<A>(options)(value))

export const deserializeRegistry = <A = unknown>(
  payload: Uint8Array,
  options?: Avro.ParseOptions
): Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.registryDeserializer<A>(options)(payload))

export const deserializeRegistryKey = deserializeRegistry
export const deserializeRegistryValue = deserializeRegistry

export const decodeKey = <A = unknown>(
  message: KafkaMessage,
  options?: Avro.ParseOptions
): Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.decodeMessageKey<A>(message, options))

export const decodeValue = <A = unknown>(
  message: KafkaMessage,
  options?: Avro.ParseOptions
): Effect.Effect<A, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.decodeMessageValue<A>(message, options))

export const encodeKey = <A>(
  message: Pick<KafkaMessage, "topic">,
  schema: Avro.AvroSchema,
  value: A,
  options?: Omit<RegistrySerializerOptions, "topic" | "schema" | "isKey">
): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.encodeMessagePayload(message, "key", schema, value, options))

export const encodeValue = <A>(
  message: Pick<KafkaMessage, "topic">,
  schema: Avro.AvroSchema,
  value: A,
  options?: Omit<RegistrySerializerOptions, "topic" | "schema" | "isKey">
): Effect.Effect<Uint8Array, Registry.SchemaRegistryError | Avro.AvroError | KafkaAvroError, KafkaAvro> =>
  KafkaAvro.use((kafka) => kafka.encodeMessagePayload(message, "value", schema, value, options))

const resolveSubject = (options: RegistrySerializerOptions): string => {
  if (options.subject !== undefined) {
    return options.subject
  }
  if (options.topic === undefined) {
    throw new KafkaAvroError("Kafka registry serializer requires either subject or topic")
  }
  return Registry.subjectName(options.subjectNameStrategy ?? "TopicNameStrategy", {
    topic: options.topic,
    schema: options.schema,
    ...(options.isKey === undefined ? {} : { isKey: options.isKey })
  })
}

const messageContext = (message: KafkaMessage, location: KafkaPayloadLocation): KafkaErrorContext => ({
  topic: message.topic,
  location,
  ...(message.partition === undefined ? {} : { partition: message.partition }),
  ...(message.offset === undefined ? {} : { offset: message.offset })
})

const message = (error: unknown): string => error instanceof Error ? error.message : String(error)
