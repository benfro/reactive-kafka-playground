# Reactor API and reactive programming

## Why reactive?
To achieve non-blocking behavior, mostly in web contexts.

## Reactive nature
Push-pull hybrid of streams of data, normally the producers doesn't start emitting 
until they are subscribed to.

## Building blocks and terms
- Producer - emits stuff
- Subscription - object sent to consumers
- Consumer - consumes what producers emit
- Backpressure - mechanism for consumers to tell producers to back off
- Sink - object sent to emit stuff in producers
- Processor - in-out object, both consumer and producer (not really a reactive term)

## Mono<?> and Flux<?>
- Mono can contain one object or be empty
- Flux can contain 0,1 0r n objects
- Bot have diverse methods of creation and of emitting items programatically, check APIs
- https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
- https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html

## Operators
There are a multitude of operators to
- handle items
- invoke callbacks ("do" operators)
- delay emission
- react to timeouts
- subscribe
- handle errors
- switch conditionally
- transform

## Hot & Cold publishers
A cold publisher does not emit anything until subscribed to.  
A hot publisher publishes anyway and directly.

## Combining publishers
There are ways of combining publishers like
- startWith
- concatWith
- merge
- zip
- flatMap
- concatMap
- collectList
- then

## Repeat and Retry
- Repeat repeats a successful operation
- Retry retries a failed operation
- Both can be done conditionally and with delays and timeouts

## Sinks
Objects that are used by publishers to emit items. Things to remember:
- unicast
- many.unicast
- many.multicast
- many.replay





