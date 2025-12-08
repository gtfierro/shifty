use serde_json;
use shacl_ir::ShapeIR;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Persist a `ShapeIR` to disk using JSON serialization.
pub fn write_shape_ir(path: &Path, shape_ir: &ShapeIR) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, shape_ir).map_err(|e| {
        Box::new(std::io::Error::other(format!(
            "Failed to serialize ShapeIR: {}",
            e
        ))) as Box<dyn Error>
    })?;
    Ok(())
}

/// Load a `ShapeIR` from disk.
pub fn read_shape_ir(path: &Path) -> Result<ShapeIR, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let shape_ir: ShapeIR = serde_json::from_reader(reader).map_err(|e| {
        Box::new(std::io::Error::other(format!(
            "Failed to deserialize ShapeIR: {}",
            e
        ))) as Box<dyn Error>
    })?;
    Ok(shape_ir)
}
