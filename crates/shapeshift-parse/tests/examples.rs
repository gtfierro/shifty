use shapeshift_macro::diagnostic::render_all;
use shapeshift_parse::{format, parse};
use std::path::PathBuf;

fn examples() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/shapeshift")
}

/// Every `.shm` file under `examples/shapeshift`, subdirectories included.
fn shm_files() -> Vec<PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|e| e == "shm") {
                out.push(p);
            }
        }
    }
    let mut v = Vec::new();
    walk(&examples(), &mut v);
    v.sort();
    v
}

#[test]
fn examples_parse_without_diagnostics() {
    for path in shm_files() {
        let src = std::fs::read_to_string(&path).unwrap();
        let out = parse(&src, &path.display().to_string());
        assert!(
            out.diagnostics.is_empty(),
            "{}:\n{}",
            path.display(),
            render_all(&out.diagnostics)
        );
    }
}

#[test]
fn examples_round_trip_parse_format_parse() {
    for path in shm_files() {
        let src = std::fs::read_to_string(&path).unwrap();
        let label = path.display().to_string();
        let once = format(&src, &label).unwrap();
        let twice = format(&once, &label).unwrap_or_else(|d| {
            panic!(
                "formatted output does not reparse:\n{once}\n{}",
                render_all(&d)
            )
        });
        assert_eq!(once, twice, "formatting is not idempotent for {label}");
        // The AST survives formatting up to spans and the block-form hint.
        let a = strip(parse(&src, &label).module);
        let b = strip(parse(&once, &label).module);
        assert_eq!(a, b, "formatting changed the meaning of {label}");
    }
}

/// Clear positions so ASTs from differently laid-out text compare equal.
fn strip(m: shapeshift_macro::ast::Module) -> serde_json::Value {
    let mut v = serde_json::to_value(m).unwrap();
    fn walk(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(map) => {
                map.remove("span");
                map.remove("header_order");
                for (_, x) in map.iter_mut() {
                    walk(x);
                }
            }
            serde_json::Value::Array(xs) => xs.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut v);
    v
}
