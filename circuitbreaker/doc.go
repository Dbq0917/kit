// Package circuitbreaker implements the circuit breaker pattern.
//
// Circuit breakers prevent thundering herds, and improve resiliency against
// intermittent errors. Every client-side endpoint should be wrapped in a
// circuit breaker.
//
// We provide several implementations in this package, but if you're looking
// for guidance, Gobreaker is probably the best place to start.  It has a
// simple and intuitive API, and is well-tested.
//
// # Composing with rate limiters
//
// Middlewares in this package treat ratelimit.ErrLimited as a rejection
// rather than a failure, so rate limit rejections never count against the
// breaker's error count, regardless of how the two middlewares are ordered
// in the chain. Each middleware keeps its own statistics: the breaker only
// observes genuine downstream errors, and the rate limiter's configuration
// is never influenced by the breaker's state.
//
// When ordering the chain, placing the circuit breaker outside of the rate
// limiter (closer to the client) is recommended: requests rejected by an
// open breaker then never reach the limiter, so they don't consume its
// tokens.
package circuitbreaker
