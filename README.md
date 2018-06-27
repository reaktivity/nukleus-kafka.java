# Kafka Nukleus Implementation

[![Build Status][build-status-image]][build-status]

[build-status-image]: https://travis-ci.org/reaktivity/nukleus-kafka.java.svg?branch=develop
[build-status]: https://travis-ci.org/reaktivity/nukleus-kafka.java

Fetching messages is supported as follows:

```
client nukleus -> BEGIN with extension data -> nukleus-kafka -> (other nuklei) -> network -> Kafka (cluster)
               <- DATA frames with ext data <-               <-                <-
```
### Route (incoming from controller)

The extension data contains the following fields, all optional:

- `topicName`: if specified, only subscriptions to this topic will be allowed (i.e. BEGIN extension data must specify this topic)
- `deltaProperty`: if specified, the given message property will be used to generate delta messages (see Delta Messages below)
- `headers` (key=value pairs): if specified, only subscriptions including all of these header conditions will be allowed (BEGIN extension must include all these header value pairs, and may include others). In addition, for a compacted topic, only messages matching these conditions will be cached (because only those messages will ever be delivered to users). Limiting the number of messages cached in order to make caching more efficient is a major motivation for specifying headers on a route.

### BEGIN (incoming from client)

The extension data contains the following fields:

- `topicName` (required): messages will be fetched from this topic
- `fetchOffsets` (required): message will be fetched starting from the given offsets. The first offset is assumed to be partition 0, the next partition 1, etc. If the number of partitions over which the topic is spread exceeds the number of given offsets, offset zero is assumed for the excess partitions.
- `fetchKey` (optional): if specified then only messages whose message key matches this value will be sent back to the client.
- `headers` (optional): if specified, each header (key and value) represents a condition which messages must match in order to be sent back to the client. All conditions must be met. For example, {{"header1", "value1"}, {"header2", "value2"}} means only messages containing headers "header1" and "header2" with values "value1" and "value2" respectively will be sent back to the client.

### DATA (reply stream from nukleus-kafka)

Each DATA frame represents the value of one Kafka message (a.k.a. record). The value is the message value from Kafka, or, if delta messages are being used, it may be the value of the message property specified in the route extension `deltaProperty` field (see Delta Messages). The extension data contains the following fields:
- `flags`: 
- `timestamp`: the timestamp of the message (as set in Kafka)
- `fetchOffsets`: gives the high watermark offsets which could be used subsequently to fetch all messages following this message (if the client disconnect and reconnects later).
- `messageKey`: the message key (as set in Kafka)

### Compacted Topics

Topics which are configured in Kafka with property "cleanup.policy" set to "compact" are treated specially, in the following ways:

- A cache of message offsets is maintained in order to enhance performance for subscriptions to a particular message key, and where possible only deliver the latest message for the key.
- This cache is kept up to date all the time by doing proactive fetches, unless this turned off by setting system property `nukleus.kafka.topic.bootstrap.enabled` to "false".
- A message cache with configurable size is used to store the latest message for each key.  This allows most subscriptions to a particular message key or to the whole topic to be satisfied without accessing the Kafka broker, assuming the cache is large enough to fit the latest message for each key, 
- Messages on compacted topics are cached during bootstrap. This is limited to topics whose routes include header conditions, unless system property `nukleus.kafka.message.cache.proactive` is set to "true" to force caching for all compacted topics.

### Configuration

The following system properties are currently supported for configuration:

- `nukleus.kafka.fetch.max.bytes` (integer, default 50 MiB): maximum value that will be specified as fetch.max.bytes in fetch requests made to Kafka.
- `nukleus.kafka.fetch.partition.max.bytes` (integer, default 1 MiB): maximum size of a partition response. Should be set to the highest configured value for Kafka broker or topic configuration property "max.message.bytes".
- `nukleus.kafka.topic.bootstrap.enabled` (boolean default true): caching of message keys and latest offsets is enabled for compacted topics to improve performance.
- `nukles.kafka.message.cache.capacity` (integer, default 128 MiB, must be a power of 2, maximum permitted value 0x40000000 = 1GiB): memory to be used for the message cache. When the limit is reached, messaes are evicted on a least recently used basis.
- `nukles.kafka.message.cache.block.capacity` (integer, default 1024): minimum allocation size for a cached message. The default value should be suitable for most purposes.

### Delta Messages
When a client subscribes to a compacted topic on a route with a value specified for `deltaProperty`, the second and subsequent message for each message key will contain the value of that message property instead of the normal message value. This allows messages to be delived as deltas or differences compared to the previous message to reduce network bandwidth. For example, if the message values are in json format, the delta property may use json patch format (RFC  6902). It is the responsibility of the message publisher to generate the appropriate value for the message property. Messages without the delta property are always delivered normally, that is, with the message value set to the message value from Kafka.
