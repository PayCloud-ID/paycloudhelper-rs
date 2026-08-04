use std::fmt;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

use crate::timer::GoTimer;

pub(crate) struct GoEventFormat;

impl<S, N> FormatEvent<S, N> for GoEventFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = EventFields::default();
        event.record(&mut fields);
        let title = if fields.pc_level.as_deref() == Some("fatal") {
            "[FTAL]"
        } else {
            level_title(*event.metadata().level())
        };

        write!(writer, "{title} ")?;
        tracing_subscriber::fmt::time::FormatTime::format_time(&GoTimer, &mut writer)?;
        writer.write_char(' ')?;
        if !fields.prefix.is_empty() {
            writer.write_str(fields.prefix.trim_end())?;
            if !fields.message.is_empty() {
                writer.write_char(' ')?;
            }
        }
        writer.write_str(&fields.message)?;
        for (name, value) in fields.rest {
            write!(writer, " {name}={value}")?;
        }
        writer.write_char('\n')
    }
}

fn level_title(level: Level) -> &'static str {
    match level {
        Level::ERROR => "[ERRO]",
        Level::WARN => "[WARN]",
        Level::INFO => "[INFO]",
        Level::DEBUG | Level::TRACE => "[DBUG]",
    }
}

#[derive(Default)]
struct EventFields {
    prefix: String,
    message: String,
    pc_level: Option<String>,
    rest: Vec<(&'static str, String)>,
}

impl Visit for EventFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value.to_string());
    }
}

impl EventFields {
    fn record(&mut self, field: &Field, value: String) {
        match field.name() {
            "prefix" => self.prefix = value,
            "message" => self.message = value,
            "pc_level" => self.pc_level = Some(value),
            name => self.rest.push((name, value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn titles_match_golog_fixed_width_contract() {
        assert_eq!(level_title(Level::ERROR), "[ERRO]");
        assert_eq!(level_title(Level::WARN), "[WARN]");
        assert_eq!(level_title(Level::INFO), "[INFO]");
        assert_eq!(level_title(Level::DEBUG), "[DBUG]");
    }
}
