# avro-effect-schema

Effect v4 native Avro codec utilities.

```ts
import { Schema } from "effect"
import { avro, Long } from "avro-effect-schema"

class User extends Schema.Class<User>("User")({
  id: Long,
  name: Schema.String
}) {}

const UserAvro = avro(User)
const encode = Schema.encodeSync(UserAvro)
const decode = Schema.decodeUnknownSync(UserAvro)

const buffer = encode(new User({ id: 1, name: "Ada" }))
const user = decode(buffer)
```

The package can:

- compile Effect Schema v4 schemas to Avro JSON schemas
- build Effect schemas from Avro JSON schemas
- encode and decode Avro binary buffers through `avro-js`
- adapt `avro-js` wrapped unions to normal Effect values
- omit and restore `_tag` literal fields for Avro records

