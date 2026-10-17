//! Embed every file under `ui/` into the binary for the opt-in dashboard.
//!
//! Generates `$OUT_DIR/ui_assets.rs` with a sorted `UI_ASSETS` table of
//! `(path relative to ui/, bytes)`.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let ui_dir = manifest_dir.join("ui");
    println!("cargo:rerun-if-changed=ui");

    let mut files = Vec::new();
    collect(&ui_dir, &mut files);
    // Sorted by the served path so the runtime can binary-search the table.
    let mut assets: Vec<(String, PathBuf)> = files
        .into_iter()
        .map(|file| {
            let relative = file
                .strip_prefix(&ui_dir)
                .expect("file under ui/")
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            (relative, file)
        })
        .collect();
    assets.sort();

    let mut table = String::from("pub static UI_ASSETS: &[(&str, &[u8])] = &[\n");
    for (relative, file) in &assets {
        println!("cargo:rerun-if-changed={}", file.display());
        let _ = writeln!(
            table,
            "    ({relative:?}, include_bytes!({:?})),",
            file.display().to_string()
        );
    }
    table.push_str("];\n");

    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR")).join("ui_assets.rs");
    fs::write(out, table).expect("write ui_assets.rs");
}
