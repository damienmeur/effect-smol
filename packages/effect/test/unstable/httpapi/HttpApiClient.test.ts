import { assert, describe, it } from "@effect/vitest"
import { strictEqual } from "@effect/vitest/utils"
import { Cause, Effect, Schema, Stream } from "effect"
import { Sse } from "effect/unstable/encoding"
import { HttpClient, HttpClientResponse } from "effect/unstable/http"
import { HttpApi, HttpApiClient, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/unstable/httpapi"

const textEncoder = new TextEncoder()

const StreamError = Schema.Struct({ reason: Schema.String })

const Events = Schema.Struct({
  event: Schema.String,
  data: Schema.String
})

const EndpointError = Schema.Struct({
  _tag: Schema.Literal("EndpointError"),
  message: Schema.String
}).pipe(HttpApiSchema.status(400))

const StreamingApi = HttpApi.make("StreamingApi").add(
  HttpApiGroup.make("test")
    .add(
      HttpApiEndpoint.get("events", "/events", {
        success: HttpApiSchema.StreamSse({ events: Events, error: StreamError }),
        error: EndpointError
      }),
      HttpApiEndpoint.get("download", "/download", {
        success: HttpApiSchema.StreamUint8Array(),
        error: EndpointError
      })
    )
)

const clientFromResponse = (response: () => Response) =>
  HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, response())))

const textStream = (chunks: ReadonlyArray<string>): ReadableStream<Uint8Array> => {
  let index = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index === chunks.length) {
        controller.close()
      } else {
        controller.enqueue(textEncoder.encode(chunks[index++]!))
      }
    }
  })
}

const byteStream = (chunks: ReadonlyArray<Uint8Array>): ReadableStream<Uint8Array> => {
  let index = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index === chunks.length) {
        controller.close()
      } else {
        controller.enqueue(chunks[index++]!)
      }
    }
  })
}

describe("HttpApiClient", () => {
  describe("streaming responses", () => {
    it.effect("decodes StreamSse events incrementally", () =>
      Effect.gen(function*() {
        const client = yield* HttpApiClient.makeWith(StreamingApi, {
          baseUrl: "http://test",
          httpClient: clientFromResponse(() =>
            new Response(
              textStream([
                "event: first\ndata: one\n\n",
                "event: second\ndata: two\n\n"
              ]),
              { status: 200 }
            )
          )
        })

        const stream = yield* client.test.events({})
        const first = yield* stream.pipe(Stream.take(1), Stream.runCollect)
        assert.deepStrictEqual(Array.from(first), [{ event: "first", data: "one" }])
      }))

    it.effect("decodes StreamSse reserved failure events as full causes", () =>
      Effect.gen(function*() {
        const expectedCause = Cause.fail({ reason: "boom" })
        const encodeCause = Schema.encodeUnknownEffect(Schema.toCodecJson(Schema.Cause(StreamError, Schema.Defect)))
        const encodedCause = yield* encodeCause(expectedCause)
        const failureEvent = Sse.encoder.write({
          _tag: "Event",
          event: "effect/httpapi/stream/failure",
          id: undefined,
          data: JSON.stringify(encodedCause)
        })
        const client = yield* HttpApiClient.makeWith(StreamingApi, {
          baseUrl: "http://test",
          httpClient: clientFromResponse(() => new Response(textStream([failureEvent]), { status: 200 }))
        })

        const stream = yield* client.test.events({})
        const exit = yield* stream.pipe(Stream.runCollect, Effect.exit)
        assert.strictEqual(exit._tag, "Failure")
        if (exit._tag === "Failure") {
          assert.deepStrictEqual(exit.cause, expectedCause)
        }
      }))

    it.effect("returns StreamUint8Array response bytes incrementally", () =>
      Effect.gen(function*() {
        const client = yield* HttpApiClient.makeWith(StreamingApi, {
          baseUrl: "http://test",
          httpClient: clientFromResponse(() =>
            new Response(byteStream([new Uint8Array([1, 2]), new Uint8Array([3])]), { status: 200 })
          )
        })

        const stream = yield* client.test.download({})
        const first = yield* stream.pipe(Stream.take(1), Stream.runCollect)
        assert.deepStrictEqual(Array.from(first, (chunk) => Array.from(chunk)), [[1, 2]])
      }))

    it.effect("decodes non-success responses through endpoint error schemas before returning a stream", () =>
      Effect.gen(function*() {
        const client = yield* HttpApiClient.makeWith(StreamingApi, {
          baseUrl: "http://test",
          httpClient: clientFromResponse(() =>
            new Response(JSON.stringify({ _tag: "EndpointError", message: "bad request" }), {
              status: 400,
              headers: { "content-type": "application/json" }
            })
          )
        })

        const error = yield* Effect.flip(client.test.events({}))
        assert.deepStrictEqual(error, { _tag: "EndpointError", message: "bad request" })
      }))

    it.effect("preserves response-only raw response stream access", () =>
      Effect.gen(function*() {
        const client = yield* HttpApiClient.makeWith(StreamingApi, {
          baseUrl: "http://test",
          httpClient: clientFromResponse(() =>
            new Response(byteStream([new Uint8Array([1]), new Uint8Array([2, 3])]), { status: 200 })
          )
        })

        const response = yield* client.test.download({ responseMode: "response-only" })
        const chunks = yield* response.stream.pipe(Stream.runCollect)
        assert.deepStrictEqual(Array.from(chunks, (chunk) => Array.from(chunk)), [[1], [2, 3]])
      }))
  })

  describe("urlBuilder", () => {
    const Api = HttpApi.make("Api")
      .add(
        HttpApiGroup.make("users")
          .add(
            HttpApiEndpoint.get("getUser", "/users/:id", {
              params: {
                id: Schema.Finite
              },
              query: {
                page: Schema.Finite,
                tags: Schema.Array(Schema.Finite)
              }
            }),
            HttpApiEndpoint.get("health", "/health")
          )
      )

    it("builds urls using endpoint schemas", () => {
      const builder = HttpApiClient.urlBuilder(Api, {
        baseUrl: "https://api.example.com"
      })

      strictEqual(
        builder.users.getUser({
          params: {
            id: 123
          },
          query: {
            page: 1,
            tags: [1, 2]
          }
        }),
        "https://api.example.com/users/123?page=1&tags=1&tags=2"
      )
    })

    it("returns relative urls when baseUrl is omitted", () => {
      const builder = HttpApiClient.urlBuilder(Api)

      strictEqual(builder.users.health(), "/health")
    })

    it("supports top-level endpoints", () => {
      const TopLevelApi = HttpApi.make("Api")
        .add(
          HttpApiGroup.make("top", { topLevel: true })
            .add(
              HttpApiEndpoint.get("health", "/health")
            )
        )
        .prefix("/v1")

      const builder = HttpApiClient.urlBuilder(TopLevelApi, {
        baseUrl: "https://api.example.com"
      })

      strictEqual(builder.health(), "https://api.example.com/v1/health")
    })
  })
})
