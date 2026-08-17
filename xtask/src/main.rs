//! Workspace dev tasks. Run via `cargo xtask <task>` (alias in `.cargo/config.toml`).

mod lint_mirrors;

use std::process::ExitCode;

const USAGE: &str = "\
cargo xtask <task>

Tasks:
  lint-mirrors [--all] [--update-baseline]
      Check that exported items in port-critical crates record what Go symbol
      they were checked against.

      --all              report every unannotated item, ignoring the baseline
      --update-baseline  rewrite the baseline to match reality (use when paying
                         debt down, and when adding a crate to the scope)
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("lint-mirrors") => lint_mirrors::run(&args[1..]),
        Some("-h" | "--help" | "help") | None => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("unknown task: {other}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}
