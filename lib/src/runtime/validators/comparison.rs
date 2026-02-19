use oxigraph::model::vocab::xsd;
use oxigraph::model::{LiteralRef, Term};
use oxsdatatypes::Decimal;
use std::cmp::Ordering;
use std::str::FromStr;

fn decimal_from_literal(lit: LiteralRef<'_>) -> Option<Decimal> {
    let datatype = lit.datatype();
    if datatype == xsd::INTEGER || datatype == xsd::DECIMAL {
        Decimal::from_str(lit.value()).ok()
    } else {
        None
    }
}

fn float_from_literal(lit: LiteralRef<'_>) -> Option<f64> {
    let datatype = lit.datatype();
    if datatype == xsd::FLOAT || datatype == xsd::DOUBLE {
        lit.value()
            .parse::<f64>()
            .ok()
            .filter(|value| !value.is_nan())
    } else {
        None
    }
}

/// Returns a total/partial ordering for common numeric literal pairs.
///
/// Falls back to `None` for non-literal terms or unsupported datatype mixes.
pub(crate) fn compare_terms_fast(left: &Term, right: &Term) -> Option<Ordering> {
    let (Term::Literal(left_lit), Term::Literal(right_lit)) = (left, right) else {
        return None;
    };

    if let (Some(left_dec), Some(right_dec)) = (
        decimal_from_literal(left_lit.as_ref()),
        decimal_from_literal(right_lit.as_ref()),
    ) {
        return left_dec.partial_cmp(&right_dec);
    }

    if let (Some(left_float), Some(right_float)) = (
        float_from_literal(left_lit.as_ref()),
        float_from_literal(right_lit.as_ref()),
    ) {
        return left_float.partial_cmp(&right_float);
    }

    None
}
