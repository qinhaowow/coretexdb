//! The version lives in `Cargo.toml`; everything else should follow it.
//!
//! This existed as a convention rather than a rule. `release.yml` named
//! `V0.2.4` in three places, the install scripts named it in three more, and
//! the previous release had to edit 17 files to make them agree — nothing
//! failed if one was missed. A release could ship a package called
//! `CoreTexDB-V0.2.4-<target>` holding a 0.2.5 binary, and the failure would
//! surface to whoever tried to install it.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The units and logrotate rules are the one place a literal belongs: they are
/// copied into an install root and handed straight to systemd, so they must
/// work without a templating step. That makes consistency the requirement
/// instead of derivation — if they name a different release than the one being
/// built, `ExecStart=` points into a directory that does not exist and the
/// service fails to start with nothing in the journal to explain it.
///
/// `install.sh` rewrites them for a prefix other than the default, so a custom
/// install is fine; a *stale* one is not.
#[test]
fn shipped_units_name_the_current_release() {
    let built = env!("CARGO_PKG_VERSION");
    let expected = format!("CoreTexDB-V{built}");

    let mut checked = 0;
    for dir in ["systemd", "logrotate"] {
        let root = repo_root().join(dir);
        let entries = std::fs::read_dir(&root)
            .unwrap_or_else(|e| panic!("{dir}/ must be readable: {e}"));

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            checked += 1;

            for (lineno, line) in text.lines().enumerate() {
                if let Some(at) = line.find("CoreTexDB-V") {
                    let rest = &line[at + "CoreTexDB-V".len()..];
                    let version: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_digit() || *c == '.')
                        .collect();
                    assert_eq!(
                        version, built,
                        "{}:{} names CoreTexDB-V{version}, but this build is {built}.\n\
                         A unit whose ExecStart points at another release's directory \
                         fails to start with no useful error.",
                        path.display(),
                        lineno + 1,
                    );
                }
            }
        }
    }

    assert!(checked > 0, "no unit or logrotate file was examined");
}

/// `VERSION` ships inside the install root, so `scripts/install.sh` reads it to
/// lay the tree out and `upgrade.sh` prints it. If it drifts from
/// `Cargo.toml`, an installed tree reports one version while the binary in it
/// reports another.
#[test]
fn version_file_matches_cargo_toml() {
    let path = repo_root().join("VERSION");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("VERSION must be readable at {}: {e}", path.display()));

    // Trim rather than compare exactly: the file is checked out with whatever
    // line endings the contributor's editor chose, and that is not a version
    // mismatch.
    let declared = raw.trim();
    let built = env!("CARGO_PKG_VERSION");

    assert_eq!(
        declared, built,
        "VERSION says {declared:?} but Cargo.toml builds {built:?}. \
         Update VERSION in the same commit as the Cargo.toml version."
    );
}

/// Anything that names the version in a path a user will type has to read it
/// instead. These are the files where a stale copy produces a wrong install
/// root rather than a wrong sentence in prose.
#[test]
fn packaging_files_derive_the_version() {
    let files = [
        "scripts/install.sh",
        "scripts/uninstall.sh",
        "scripts/secure_setup.sh",
        ".github/workflows/release.yml",
    ];

    for rel in files {
        let path = repo_root().join(rel);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} must be readable: {e}", rel));

        for (lineno, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            // A comment or usage line may name an example path; what matters is
            // that no *executable* line builds one from a literal.
            if trimmed.starts_with('#') {
                continue;
            }
            assert!(
                !regex_lite_version_literal(line),
                "{rel}:{} names a version literally: {}\n\
                 Read it from Cargo.toml or $VERSION instead.",
                lineno + 1,
                trimmed
            );
        }
    }
}

/// Cheap "does this line contain a V0.2.x literal" check, written out rather
/// than pulled in as a regex dependency: the pattern is fixed and the crate has
/// no business taking a dev-dependency for one assertion.
fn regex_lite_version_literal(line: &str) -> bool {
    const NEEDLE: &[u8] = b"V0.2.";
    let bytes = line.as_bytes();

    bytes.windows(NEEDLE.len()).enumerate().any(|(at, window)| {
        // A digit right after "V0.2." is what makes this a version rather than
        // a coincidence; `${v}` and `V0.2.x` are both fine to leave alone.
        window == NEEDLE && bytes.get(at + NEEDLE.len()).is_some_and(u8::is_ascii_digit)
    })
}

/// The C header repeats the version as three `#define`s, which no build step
/// rewrites. `tests/ffi_api.rs` already guards them — and did, by failing when
/// 0.2.5 was cut and only `Cargo.toml` moved. This gate exists to catch the
/// same thing without a red suite being the notice.
#[test]
fn c_header_declares_the_current_version() {
    let built = env!("CARGO_PKG_VERSION");
    let mut parts = built.split('.').filter(|p| !p.is_empty());

    let header = std::fs::read_to_string(repo_root().join("include/coretexdb.h"))
        .expect("include/coretexdb.h must be readable");

    for label in ["MAJOR", "MINOR", "PATCH"] {
        let expected = parts
            .next()
            .unwrap_or_else(|| panic!("{built} is not a three-part version"));
        let needle = format!("#define CORETEXDB_VERSION_{label} {expected}");
        assert!(
            header.contains(&needle),
            "include/coretexdb.h is missing `{needle}` — it still describes {built}'s \
             predecessor, so a C caller compiles against the wrong version."
        );
    }
}

/// `DB_VERSION` is the constant every embedder reads, so it must be the crate's
/// own version rather than a literal that can go stale on its own.
#[test]
fn public_version_constant_tracks_the_crate() {
    assert_eq!(
        coretexdb::DB_VERSION,
        env!("CARGO_PKG_VERSION"),
        "DB_VERSION and CARGO_PKG_VERSION disagree"
    );
}

#[test]
fn version_file_has_no_stray_content() {
    let path: PathBuf = repo_root().join("VERSION");
    let raw = std::fs::read_to_string(&path).expect("VERSION must be readable");

    // `cat VERSION` is used directly in scripts and in the release body, so a
    // second line would leak into an install log or a shell substitution.
    assert_eq!(
        raw.lines().filter(|l| !l.trim().is_empty()).count(),
        1,
        "VERSION must hold exactly one line, got {raw:?}"
    );
    assert!(
        Path::new(&path).is_file(),
        "VERSION must be a plain file, not a directory"
    );
}
