//! Log timestamp formatter.
//!
//! mirrors: Go `Log.SetTimeFormat("2006-01-02 15:04:05.000")` from
//! `phlogger.InitializeLogger`, i.e. Rust strftime `%Y-%m-%d %H:%M:%S%.3f`.
//!
use std::fmt;

use time::macros::format_description;
use time::OffsetDateTime;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

/// Timer that renders `%Y-%m-%d %H:%M:%S%.3f` in the process local zone.
pub(crate) struct GoTimer;

impl FormatTime for GoTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
        let format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"
        );
        let rendered = now.format(&format).map_err(|_| fmt::Error)?;
        w.write_str(&rendered)
    }
}
