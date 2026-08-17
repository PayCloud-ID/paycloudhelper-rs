//! `cargo xtask lint-mirrors` — require every exported item in a port-critical
//! crate to record what it was checked against in Go.
//!
//! # Why this exists
//!
//! This workspace is a hand-written port of a Go library. Four port omissions
//! reached a release in a single fortnight — `pc-amqp` dropped Go's passive queue
//! probe (CrashLoopBackOff), `pc-config` checked env names PayCloud never
//! deploys, `pc-redis` dropped Go's per-operation timeouts, and `pc-snapbi`'s
//! JWT validation was disabled on a false premise. None were caught by review,
//! because nothing in the source recorded *whether anyone had read the Go
//! original*.
//!
//! Where that record exists it is a `/// mirrors: <GoSymbol>` doc comment. All
//! four defects landed in crates with **zero** of them.
//!
//! # What it does NOT claim
//!
//! An annotation proves documentation, not correctness — `pc-audit` has none and
//! passed a detailed hand check. This is a prompt to look, not evidence of
//! having looked well. It is cheap and it aims at the right crates, which is the
//! most that can be asked of a lint.
//!
//! # Why a baseline instead of a clean sweep
//!
//! There are ~125 unannotated exports in scope. A lint that fails on all of them
//! from day one gets switched off within a week. So the accepted debt is
//! recorded in `xtask/mirrors-baseline.txt` and the lint fails only on items
//! **not** in it. New exports must be annotated; old ones are paid down at
//! leisure. The ratchet only turns one way — a stale baseline entry is also an
//! error, so the file cannot quietly rot as items are renamed or deleted.
//!
//! # Annotation grammar
//!
//! ```text
//! /// mirrors: `phhelper.BuildLogPrefix`
//! /// mirrors: `phsentry.InitSentry` + `phsentry.InitSentryOptions`
//! /// no-go-equivalent: Go has no half-open state; this is a port addition
//! ```
//!
//! The `+` form matters. One Rust function mirroring several Go symbols is not
//! hypothetical: `pc-snapbi::symmetric_sign` covers two Go functions that hash
//! the request body *differently*, and comparing it against only one of them
//! produced a recommendation that would have broken inbound signature
//! verification in staging. If a symbol has more than one counterpart, the
//! annotation has to say so.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Crates whose exports must carry an annotation.
///
/// Scope is deliberately the six crates with zero annotations today — the defect
/// zone. Widening it is a `--update-baseline` away, but a lint that lights up
/// every crate at once teaches people to ignore it.
const SCOPE: &[&str] = &[
    "pc-amqp",
    "pc-audit",
    "pc-auth",
    "pc-health",
    "pc-s3minio",
    "pc-snapbi",
];

const ANNOTATIONS: &[&str] = &["mirrors:", "no-go-equivalent:"];

const BASELINE: &str = "xtask/mirrors-baseline.txt";

/// One exported item that needs an annotation.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Export {
    /// `crate/path.rs:LINE:item` — stable enough to diff, precise enough to open.
    key: String,
}

pub fn run(args: &[String]) -> ExitCode {
    let show_all = args.iter().any(|a| a == "--all");
    let update = args.iter().any(|a| a == "--update-baseline");

    let root = workspace_root();
    let found = match collect(&root) {
        Ok(found) => found,
        Err(err) => {
            eprintln!("lint-mirrors: {err}");
            return ExitCode::FAILURE;
        }
    };

    if update {
        let mut out = String::from(
            "# Exported items still missing a `mirrors:` / `no-go-equivalent:` annotation.\n\
             #\n\
             # Accepted debt, not approval. Shrink it; do not grow it. Regenerate with\n\
             # `cargo xtask lint-mirrors --update-baseline` ONLY when paying debt down or\n\
             # bringing a new crate into scope — never to silence a new export.\n",
        );
        for export in &found {
            let _ = writeln!(out, "{}", export.key);
        }
        if let Err(err) = std::fs::write(root.join(BASELINE), out) {
            eprintln!("lint-mirrors: writing baseline: {err}");
            return ExitCode::FAILURE;
        }
        println!(
            "lint-mirrors: baseline updated with {} entries",
            found.len()
        );
        return ExitCode::SUCCESS;
    }

    if show_all {
        for export in &found {
            println!("{}", export.key);
        }
        println!("\n{} unannotated exported item(s) in scope", found.len());
        return ExitCode::SUCCESS;
    }

    let baseline = read_baseline(&root.join(BASELINE));
    let current: BTreeSet<&str> = found.iter().map(|e| e.key.as_str()).collect();

    let new: Vec<&&str> = current.iter().filter(|k| !baseline.contains(**k)).collect();
    let stale: Vec<&String> = baseline
        .iter()
        .filter(|k| !current.contains(k.as_str()))
        .collect();

    if new.is_empty() && stale.is_empty() {
        println!(
            "lint-mirrors: ok — {} known unannotated item(s), none new",
            found.len()
        );
        return ExitCode::SUCCESS;
    }

    if !new.is_empty() {
        eprintln!(
            "\nlint-mirrors: {} exported item(s) have no `mirrors:` or `no-go-equivalent:` annotation.\n\
             \n\
             This workspace is a port. Record what the Go original is, or say there isn't one:\n\
             \n    /// mirrors: `pkg.GoSymbol`\n\
             \n    /// mirrors: `pkg.GoOne` + `pkg.GoTwo`      (when it really is more than one)\n\
             \n    /// no-go-equivalent: <why>\n",
            new.len()
        );
        for key in &new {
            eprintln!("  {key}");
        }
    }

    if !stale.is_empty() {
        eprintln!(
            "\nlint-mirrors: {} baseline entr(y/ies) no longer match any export.\n\
             Renamed, deleted, or annotated? Either way run \
             `cargo xtask lint-mirrors --update-baseline` so the ratchet cannot rot:\n",
            stale.len()
        );
        for key in &stale {
            eprintln!("  {key}");
        }
    }

    ExitCode::FAILURE
}

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/xtask`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives one level below the workspace root")
        .to_path_buf()
}

fn collect(root: &Path) -> Result<Vec<Export>, String> {
    let mut out = Vec::new();
    for krate in SCOPE {
        let dir = root.join("crates").join(krate).join("src");
        if !dir.is_dir() {
            return Err(format!(
                "crate `{krate}` is in SCOPE but {} does not exist",
                dir.display()
            ));
        }
        let mut files = Vec::new();
        collect_rs(&dir, &mut files)?;
        files.sort();
        for file in files {
            let source = std::fs::read_to_string(&file)
                .map_err(|e| format!("reading {}: {e}", file.display()))?;
            let rel = file
                .strip_prefix(root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            scan(&source, &rel, &mut out);
        }
    }
    out.sort();
    Ok(out)
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|e| format!("reading {}: {e}", dir.display()))? {
        let entry = entry.map_err(|e| format!("reading {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Find exported items lacking an annotation in their immediately preceding doc
/// block.
///
/// Line scanning rather than `syn`: `rustdoc --output-format json` needs nightly
/// (this workspace is stable, MSRV 1.88), and a parser is a large dependency to
/// carry into `cargo deny`/`audit` for a heuristic. The cost is that this must
/// track `#[cfg(test)]` blocks by brace depth, which it does below.
fn scan(source: &str, rel_path: &str, out: &mut Vec<Export>) {
    let lines: Vec<&str> = source.lines().collect();
    let mut test_depth: Option<i32> = None;
    let mut depth: i32 = 0;

    for (index, raw) in lines.iter().enumerate() {
        let line = raw.trim();

        // Track whether we are inside a `#[cfg(test)]` module, by brace depth.
        if test_depth.is_none() && (line == "#[cfg(test)]" || line.starts_with("#[cfg(test)]")) {
            test_depth = Some(depth);
        }
        let opens = i32::try_from(raw.matches('{').count()).unwrap_or(0);
        let closes = i32::try_from(raw.matches('}').count()).unwrap_or(0);
        let depth_before = depth;
        depth += opens - closes;
        if let Some(entry_depth) = test_depth {
            if depth <= entry_depth && closes > 0 {
                test_depth = None;
            }
            continue;
        }

        let Some(item) = exported_item(line) else {
            continue;
        };
        // Nested `pub` inside a function body is not an export of interest, and
        // `pub` inside a trait impl inherits the trait's contract.
        if depth_before > 0 && !line.starts_with("pub") {
            continue;
        }
        if has_annotation(&lines, index) {
            continue;
        }
        out.push(Export {
            key: format!("{rel_path}:{}:{item}", index + 1),
        });
    }
}

/// Return the item's name when `line` declares an exported item.
fn exported_item(line: &str) -> Option<String> {
    if line.starts_with("pub(crate)") || line.starts_with("pub(super)") {
        return None;
    }
    let rest = line.strip_prefix("pub ")?;
    // Skip re-exports and modules: `pub use` carries the annotation at the
    // definition, and `pub mod` is a container, not a ported symbol.
    if rest.starts_with("use ") || rest.starts_with("mod ") {
        return None;
    }
    let rest = rest
        .strip_prefix("async ")
        .or_else(|| rest.strip_prefix("unsafe "))
        .unwrap_or(rest);
    for keyword in [
        "fn ", "struct ", "enum ", "trait ", "type ", "const ", "static ", "union ",
    ] {
        if let Some(tail) = rest.strip_prefix(keyword) {
            let name: String = tail
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                return None;
            }
            return Some(name);
        }
    }
    None
}

/// Walk backwards over the doc block and attributes attached to `index`.
fn has_annotation(lines: &[&str], index: usize) -> bool {
    let mut i = index;
    while i > 0 {
        i -= 1;
        let line = lines[i].trim();
        if line.starts_with("///") || line.starts_with("//!") || line.starts_with("//") {
            if ANNOTATIONS.iter().any(|a| line.contains(a)) {
                return true;
            }
            continue;
        }
        // Attributes and derives sit between the docs and the item.
        if line.starts_with('#')
            || line.is_empty() && i > 0 && lines[i - 1].trim().starts_with("//")
        {
            continue;
        }
        break;
    }
    false
}

fn read_baseline(path: &Path) -> BTreeSet<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_exported_items() {
        assert_eq!(exported_item("pub fn thing() {").as_deref(), Some("thing"));
        assert_eq!(
            exported_item("pub async fn thing() {").as_deref(),
            Some("thing")
        );
        assert_eq!(
            exported_item("pub struct Thing {").as_deref(),
            Some("Thing")
        );
        assert_eq!(exported_item("pub const X: u8 = 1;").as_deref(), Some("X"));
        assert_eq!(exported_item("pub trait T {").as_deref(), Some("T"));
    }

    #[test]
    fn ignores_non_exports() {
        assert_eq!(exported_item("fn private() {"), None);
        assert_eq!(exported_item("pub(crate) fn internal() {"), None);
        assert_eq!(exported_item("pub(super) fn internal() {"), None);
        // Re-exports carry their annotation at the definition site.
        assert_eq!(exported_item("pub use thing::Thing;"), None);
        assert_eq!(exported_item("pub mod thing;"), None);
    }

    #[test]
    fn finds_an_annotation_through_docs_and_attributes() {
        let lines = vec![
            "/// mirrors: `pkg.GoThing`",
            "#[must_use]",
            "#[derive(Debug)]",
            "pub fn thing() {",
        ];
        assert!(has_annotation(&lines, 3));
    }

    #[test]
    fn accepts_the_multi_mirror_and_no_equivalent_forms() {
        let multi = vec!["/// mirrors: `a.One` + `a.Two`", "pub fn thing() {"];
        assert!(has_annotation(&multi, 1));
        let none = vec![
            "/// no-go-equivalent: added by this port",
            "pub fn thing() {",
        ];
        assert!(has_annotation(&none, 1));
    }

    #[test]
    fn an_undocumented_item_is_reported() {
        let lines = vec!["/// Does a thing.", "pub fn thing() {"];
        assert!(!has_annotation(&lines, 1));
    }

    /// A doc block belonging to a *previous* item must not be credited to this
    /// one — otherwise one annotation would silence a whole file.
    #[test]
    fn does_not_borrow_a_previous_items_annotation() {
        let lines = vec![
            "/// mirrors: `pkg.Other`",
            "pub fn other() {}",
            "",
            "pub fn thing() {",
        ];
        assert!(!has_annotation(&lines, 3));
    }

    #[test]
    fn test_modules_are_skipped() {
        let source = "\
pub fn real() {}

#[cfg(test)]
mod tests {
    pub fn helper() {}
}
";
        let mut out = Vec::new();
        scan(source, "x.rs", &mut out);
        let keys: Vec<&str> = out.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["x.rs:1:real"], "test-only items are not exports");
    }
}
