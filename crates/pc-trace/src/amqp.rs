//! W3C `traceparent` propagation over AMQP (RabbitMQ) message headers.
//!
//! mirrors: `phtrace/rmqprop.go` (`AMQPCarrier`, `NewAMQPCarrier`, `Get`, `Set`,
//! `Keys`, `InjectAMQP`, `ExtractAMQP`). Gated behind the `amqp` Cargo feature
//! so a traces-only service does not compile `lapin`.

use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::Context;

use crate::propagator;

/// An [`Injector`] backed by a mutable AMQP header table.
///
/// mirrors: `phtrace.AMQPCarrier` on the publish (inject) side.
struct AmqpInjector<'a> {
    headers: &'a mut FieldTable,
}

impl Injector for AmqpInjector<'_> {
    /// Writes a header value as an AMQP `LongString` (the type RabbitMQ uses for
    /// string headers). An empty key is ignored per AMQP semantics.
    ///
    /// mirrors: `(*AMQPCarrier).Set`.
    fn set(&mut self, key: &str, value: String) {
        if key.is_empty() {
            return;
        }
        self.headers.insert(
            ShortString::from(key),
            AMQPValue::LongString(LongString::from(value)),
        );
    }
}

/// An [`Extractor`] backed by a read-only AMQP header table.
///
/// mirrors: `phtrace.AMQPCarrier` on the consume (extract) side.
struct AmqpExtractor<'a> {
    headers: &'a FieldTable,
}

impl Extractor for AmqpExtractor<'_> {
    /// Returns the value for `key`, normalizing typed AMQP values to their
    /// string form. Long/short strings are returned as borrowed `&str`; other
    /// types (and invalid UTF-8) yield `None`.
    ///
    /// mirrors: `(*AMQPCarrier).Get`.
    fn get(&self, key: &str) -> Option<&str> {
        match self.headers.inner().get(key)? {
            AMQPValue::LongString(v) => std::str::from_utf8(v.as_bytes()).ok(),
            AMQPValue::ShortString(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Returns all header keys. Order follows the underlying `BTreeMap`.
    ///
    /// mirrors: `(*AMQPCarrier).Keys`.
    fn keys(&self) -> Vec<&str> {
        self.headers
            .inner()
            .keys()
            .map(ShortString::as_str)
            .collect()
    }
}

/// Writes the span context from `cx` into `headers` using the active
/// propagator (W3C `traceparent` + `baggage`). Safe to call before init — a
/// fresh composite propagator is used so the traceparent is still written.
///
/// mirrors: `phtrace.InjectAMQP` (the Rust signature takes an explicit context
/// and a `&mut FieldTable` rather than returning the table).
pub fn inject_amqp(cx: &Context, headers: &mut FieldTable) {
    let mut injector = AmqpInjector { headers };
    propagator().inject_context(cx, &mut injector);
}

/// Returns a new [`Context`] carrying the span context extracted from `headers`.
/// When no `traceparent` is present the returned context is a fresh root.
///
/// mirrors: `phtrace.ExtractAMQP` (the Rust signature omits the input context
/// and roots extraction at an empty [`Context`]).
#[must_use]
pub fn extract_amqp(headers: &FieldTable) -> Context {
    let extractor = AmqpExtractor { headers };
    propagator().extract_with_context(&Context::new(), &extractor)
}
