use crate::ir::SrcGenIR;

pub fn optimize(_ir: &mut SrcGenIR) {
    // Phase-0 keeps lowering output unchanged. Future passes can fold constants and
    // precompute specialization tables while preserving deterministic ordering.
}
