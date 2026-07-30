//! Log timestamp formatter.
//!
//! mirrors: Go `Log.SetTimeFormat("2006-01-02 15:04:05.000")` from
//! `phlogger.InitializeLogger`, i.e. Rust strftime `%Y-%m-%d %H:%M:%S%.3f`.
//!
//! Implemented with `std` only (no `chrono`/`time` dependency) so the crate
//! stays lean. NOTE: this emits UTC, whereas Go's `golog` uses the process
//! local zone; the string *shape* is identical.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

/// Timer that renders `%Y-%m-%d %H:%M:%S%.3f` in UTC.
pub(crate) struct GoTimer;

impl FormatTime for GoTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = i64::try_from(now.as_secs()).unwrap_or(0);
        let millis = now.subsec_millis();

        let days = secs.div_euclid(86_400);
        let secs_of_day = secs.rem_euclid(86_400);
        let hour = secs_of_day / 3_600;
        let minute = (secs_of_day % 3_600) / 60;
        let second = secs_of_day % 60;
        let (year, month, day) = civil_from_days(days);

        write!(
            w,
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}"
        )
    }
}

/// Converts a count of days since the Unix epoch into `(year, month, day)`.
///
/// Howard Hinnant's `civil_from_days` algorithm (public domain), valid for the
/// proleptic Gregorian calendar over the full range we care about.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (y + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::civil_from_days;

    #[test]
    fn civil_from_days_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-03-01 is 11017 days after the epoch.
        assert_eq!(civil_from_days(11_017), (2000, 3, 1));
        // 2021-01-01 is 18628 days after the epoch.
        assert_eq!(civil_from_days(18_628), (2021, 1, 1));
    }
}
