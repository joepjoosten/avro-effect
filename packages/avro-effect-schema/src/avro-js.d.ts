declare module "avro-js" {
  export interface Type {
    fromBuffer(buffer: Buffer, resolver?: unknown, noCheck?: boolean): unknown
    toBuffer(value: unknown): Buffer
    createResolver(type: Type, options?: unknown): unknown
    getSchema(noDeref?: boolean): string
    isValid(value: unknown): boolean
  }

  export function parse(schema: unknown, options?: unknown): Type
}

