//! Matching a call's arguments to a template's parameters.
//!
//! Shape templates and path templates take arguments the same way:
//!
//! ```text
//! shape on-box-or-downstream(cls, room = brick:Room, n = 1) { ... }
//!
//! on-box-or-downstream(brick:CO2_Level_Sensor, n = 0)
//! ```
//!
//! Positional arguments fill parameters left to right; `name = value`
//! arguments fill parameters by name; a block after the call
//! (`T(args) { ... }`) is the last argument. A parameter left out takes its
//! default, which may refer to the parameters before it.

use crate::ast::{Arg, Call, Param};
use crate::diagnostic::nearest;

/// Why a call's arguments do not fit the parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgError {
    pub message: String,
    pub hint: Option<String>,
}

fn err(message: String, hint: Option<String>) -> ArgError {
    ArgError { message, hint }
}

/// `(a, b = ex:x)`, for messages.
pub fn signature(params: &[Param]) -> String {
    params
        .iter()
        .map(|p| match &p.default {
            Some(d) => format!("{} = {}", p.name, crate::display::arg(d)),
            None => p.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// For each parameter, the argument the call gives it, or `None` when it
/// takes its default.
pub fn match_args(
    callee: &str,
    params: &[Param],
    call: &Call,
) -> Result<Vec<Option<Arg>>, ArgError> {
    let mut out: Vec<Option<Arg>> = vec![None; params.len()];
    let sig = || format!("`{callee}({})`", signature(params));
    if call.args.len() > params.len() {
        return Err(err(
            format!(
                "`{callee}` takes {} argument{}, but was given {}",
                params.len(),
                if params.len() == 1 { "" } else { "s" },
                call.args.len()
            ),
            Some(format!("its parameters are {}", sig())),
        ));
    }
    for (slot, a) in out.iter_mut().zip(&call.args) {
        *slot = Some(a.clone());
    }
    for n in &call.named {
        let Some(i) = params.iter().position(|p| p.name == n.name) else {
            let names = params.iter().map(|p| p.name.as_str());
            return Err(err(
                format!("`{callee}` has no parameter `{}`", n.name),
                Some(match nearest(&n.name, names) {
                    Some(m) => format!("did you mean `{m}`?"),
                    None => format!("its parameters are {}", sig()),
                }),
            ));
        };
        if out[i].is_some() {
            return Err(err(
                format!("`{}` is given twice in this call to `{callee}`", n.name),
                None,
            ));
        }
        out[i] = Some(n.value.clone());
    }
    if let Some(b) = &call.block {
        let Some(last) = params.last() else {
            return Err(err(
                format!("`{callee}` takes no arguments, so no block"),
                None,
            ));
        };
        let i = params.len() - 1;
        if out[i].is_some() {
            return Err(err(
                format!(
                    "the block after `{callee}(...)` is its last argument, `{}`, which is already given",
                    last.name
                ),
                Some(format!("its parameters are {}", sig())),
            ));
        }
        out[i] = Some(Arg::Shape(b.clone()));
    }
    let missing: Vec<&str> = params
        .iter()
        .zip(&out)
        .filter(|(p, a)| a.is_none() && p.default.is_none())
        .map(|(p, _)| p.name.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(err(
            format!(
                "`{callee}` needs {} {}",
                if missing.len() == 1 {
                    "an argument for"
                } else {
                    "arguments for"
                },
                missing
                    .iter()
                    .map(|m| format!("`{m}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Some(format!("its parameters are {}", sig())),
        ));
    }
    Ok(out)
}
