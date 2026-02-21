use crate::GeneratedRust;
use std::fs;
use std::path::Path;

pub fn write_generated_modules(out: &Path, modules: &GeneratedRust) -> Result<(), String> {
    if out.extension().and_then(|ext| ext.to_str()) == Some("rs") {
        let base_name = out
            .file_stem()
            .ok_or_else(|| "output path missing file stem".to_string())?
            .to_string_lossy();
        let module_dir = out.with_file_name(base_name.as_ref());
        write_modules_dir(&module_dir, modules)?;
        let shim = format!(
            "#[path = \"{}/mod.rs\"]\nmod generated;\npub use generated::*;\n",
            module_dir
                .file_name()
                .ok_or_else(|| "module dir missing file name".to_string())?
                .to_string_lossy()
        );
        fs::write(out, shim).map_err(|err| format!("failed to write shim {}: {err}", out.display()))
    } else {
        write_modules_dir(out, modules)
    }
}

fn write_modules_dir(dir: &Path, modules: &GeneratedRust) -> Result<(), String> {
    fs::create_dir_all(dir)
        .map_err(|err| format!("failed to create output dir {}: {err}", dir.display()))?;
    fs::write(dir.join("mod.rs"), &modules.root)
        .map_err(|err| format!("failed to write mod.rs: {err}"))?;

    for (name, content) in &modules.files {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create parent {}: {err}", parent.display()))?;
        }
        fs::write(&path, content)
            .map_err(|err| format!("failed to write {}: {err}", path.display()))?;
    }

    Ok(())
}
