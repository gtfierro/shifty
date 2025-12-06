use super::model::ShapesModel;
use super::validation::ValidationContext;
use crate::model::components::ComponentDescriptor;
use crate::runtime::build_component_from_descriptor;
use crate::types::TraceItem;
use oxigraph::model::Term;
use std::collections::HashMap;

pub(crate) fn sanitize_graphviz_string(input: &str) -> String {
    input.chars().filter(|c| c.is_alphanumeric()).collect()
}

pub(crate) fn format_term_for_label(term: &Term) -> String {
    match term {
        Term::NamedNode(nn) => {
            let iri_str = nn.as_str();
            if let Some(hash_idx) = iri_str.rfind('#') {
                iri_str[hash_idx + 1..].to_string()
            } else if let Some(slash_idx) = iri_str.rfind('/') {
                if slash_idx == iri_str.len() - 1 && iri_str.len() > 1 {
                    let without_trailing_slash = &iri_str[..slash_idx];
                    if let Some(prev_slash_idx) = without_trailing_slash.rfind('/') {
                        without_trailing_slash[prev_slash_idx + 1..].to_string()
                    } else {
                        without_trailing_slash.to_string()
                    }
                } else {
                    iri_str[slash_idx + 1..].to_string()
                }
            } else {
                iri_str.to_string()
            }
        }
        Term::BlankNode(_) => "BlankNode".to_string(),
        Term::Literal(lit) => lit.value().to_string().replace('"', "\\\""),
    }
}

pub(crate) fn render_shapes_graphviz(model: &ShapesModel) -> Result<String, String> {
    let mut dot_string = String::new();
    dot_string.push_str("digraph {\n");

    for shape in model.node_shapes.values() {
        let name = model
            .nodeshape_id_lookup
            .read()
            .unwrap()
            .get_term(*shape.identifier())
            .ok_or_else(|| format!("Missing term for nodeshape ID: {:?}", shape.identifier()))?
            .clone();
        let name_label = format_term_for_label(&name);
        dot_string.push_str(&format!(
            "  {} [label=\"NodeShape\\n{}\"];\n",
            shape.identifier().to_graphviz_id(),
            name_label
        ));
        for comp in shape.constraints() {
            dot_string.push_str(&format!(
                "    {} -> {};\n",
                shape.identifier().to_graphviz_id(),
                comp.to_graphviz_id()
            ));
        }
        for comp_id in shape.constraints() {
            if let Some(ComponentDescriptor::Property { shape: prop_id }) =
                model.component_descriptors.get(comp_id)
            {
                dot_string.push_str(&format!(
                    "    {} -> {};\n",
                    shape.identifier().to_graphviz_id(),
                    prop_id.to_graphviz_id()
                ));
            }
        }
    }

    for pshape in model.prop_shapes.values() {
        model
            .propshape_id_lookup
            .read()
            .unwrap()
            .get_term(*pshape.identifier())
            .ok_or_else(|| format!("Missing term for propshape ID: {:?}", pshape.identifier()))?;

        let path_label = pshape.sparql_path();
        dot_string.push_str(&format!(
            "  {} [label=\"PropertyShape\\nPath: {}\"];\n",
            pshape.identifier().to_graphviz_id(),
            path_label
        ));
        for comp in pshape.constraints() {
            dot_string.push_str(&format!(
                "    {} -> {};\n",
                pshape.identifier().to_graphviz_id(),
                comp.to_graphviz_id()
            ));
        }
        for comp_id in pshape.constraints() {
            if let Some(descriptor) = model.component_descriptors.get(comp_id) {
                match descriptor {
                    ComponentDescriptor::Node { shape } => {
                        dot_string.push_str(&format!(
                            "    {} -> {};\n",
                            pshape.identifier().to_graphviz_id(),
                            shape.to_graphviz_id()
                        ));
                    }
                    ComponentDescriptor::QualifiedValueShape { shape, .. } => {
                        dot_string.push_str(&format!(
                            "    {} -> {};\n",
                            pshape.identifier().to_graphviz_id(),
                            shape.to_graphviz_id()
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    for (ident, descriptor) in model.component_descriptors.iter() {
        let component = build_component_from_descriptor(descriptor);
        dot_string.push_str(&format!(
            "  {} [label=\"{}\"];\n",
            ident.to_graphviz_id(),
            component.label()
        ));
    }
    dot_string.push_str("}\n");
    Ok(dot_string)
}

pub(crate) fn render_heatmap_graphviz(
    context: &ValidationContext,
    include_all_nodes: bool,
) -> Result<String, String> {
    let mut frequencies: HashMap<TraceItem, usize> = HashMap::new();
    for trace in context.execution_traces.lock().unwrap().iter() {
        for item in trace.iter() {
            *frequencies.entry(item.clone()).or_insert(0) += 1;
        }
    }

    let total_freq = frequencies.values().sum::<usize>();
    let max_freq = frequencies.values().max().copied().unwrap_or(1) as f32;

    let get_color = |count: usize| -> String {
        if count == 0 {
            return "#FFFFFF".to_string();
        }
        let ratio = count as f32 / max_freq;
        let r = (255.0 - (255.0 - 139.0) * ratio) as u8;
        let g = (255.0 - (255.0 - 0.0) * ratio) as u8;
        let b = (255.0 - (255.0 - 0.0) * ratio) as u8;
        format!("#{:02X}{:02X}{:02X}", r, g, b)
    };

    let mut dot_string = String::new();
    dot_string.push_str("digraph {\n");
    dot_string.push_str("    node [style=filled];\n");

    for shape in context.model.node_shapes.values() {
        let trace_item = TraceItem::NodeShape(*shape.identifier());
        let count = frequencies.get(&trace_item).copied().unwrap_or(0);

        if !include_all_nodes && count == 0 {
            continue;
        }

        let color = get_color(count);
        let relative_freq = if total_freq > 0 {
            (count as f32 / total_freq as f32) * 100.0
        } else {
            0.0
        };

        let name = context
            .model
            .nodeshape_id_lookup
            .read()
            .unwrap()
            .get_term(*shape.identifier())
            .ok_or_else(|| format!("Missing term for nodeshape ID: {:?}", shape.identifier()))?
            .clone();
        let name_label = format_term_for_label(&name);
        dot_string.push_str(&format!(
            "  {} [label=\"NodeShape\\n{}\\n({:.2}%) ({}/{})\", fillcolor=\"{}\"];\n",
            shape.identifier().to_graphviz_id(),
            name_label,
            relative_freq,
            count,
            total_freq,
            color
        ));
        for comp_id in shape.constraints() {
            let comp_trace_item = TraceItem::Component(*comp_id);
            let comp_count = frequencies.get(&comp_trace_item).copied().unwrap_or(0);

            if include_all_nodes || comp_count > 0 {
                dot_string.push_str(&format!(
                    "    {} -> {};\n",
                    shape.identifier().to_graphviz_id(),
                    comp_id.to_graphviz_id()
                ));
            }
        }
    }

    for pshape in context.model.prop_shapes.values() {
        let trace_item = TraceItem::PropertyShape(*pshape.identifier());
        let count = frequencies.get(&trace_item).copied().unwrap_or(0);

        if !include_all_nodes && count == 0 {
            continue;
        }

        let color = get_color(count);
        let relative_freq = if total_freq > 0 {
            (count as f32 / total_freq as f32) * 100.0
        } else {
            0.0
        };

        context
            .model
            .propshape_id_lookup
            .read()
            .unwrap()
            .get_term(*pshape.identifier())
            .ok_or_else(|| format!("Missing term for propshape ID: {:?}", pshape.identifier()))?;

        let path_label = pshape.sparql_path();
        dot_string.push_str(&format!(
            "  {} [label=\"PropertyShape\\nPath: {}\\n({:.2}%) ({}/{})\", fillcolor=\"{}\"];\n",
            pshape.identifier().to_graphviz_id(),
            path_label,
            relative_freq,
            count,
            total_freq,
            color
        ));
        for comp_id in pshape.constraints() {
            let comp_trace_item = TraceItem::Component(*comp_id);
            let comp_count = frequencies.get(&comp_trace_item).copied().unwrap_or(0);

            if include_all_nodes || comp_count > 0 {
                dot_string.push_str(&format!(
                    "    {} -> {};\n",
                    pshape.identifier().to_graphviz_id(),
                    comp_id.to_graphviz_id()
                ));
            }
        }
    }

    for (ident, descriptor) in context.model.component_descriptors.iter() {
        let comp = build_component_from_descriptor(descriptor);
        let trace_item = TraceItem::Component(*ident);
        let count = frequencies.get(&trace_item).copied().unwrap_or(0);

        if !include_all_nodes && count == 0 {
            continue;
        }

        let color = get_color(count);
        let relative_freq = if total_freq > 0 {
            (count as f32 / total_freq as f32) * 100.0
        } else {
            0.0
        };

        let comp_str = comp.to_graphviz_string(*ident, context);
        for line in comp_str.lines() {
            let mut modified_line = line.to_string();
            if let Some(start_pos) = modified_line.find('[') {
                if let Some(end_pos) = modified_line.rfind(']') {
                    let color_attr = format!("fillcolor=\"{}\", ", color);
                    modified_line.insert_str(start_pos + 1, &color_attr);

                    if let Some(label_start) = modified_line.find("label=\"") {
                        let new_end_pos = end_pos + color_attr.len();
                        if let Some(label_end) = modified_line[..new_end_pos].rfind('"') {
                            if label_end > label_start {
                                let freq_text = format!(
                                    "\\n({:.2}%) ({}/{})",
                                    relative_freq, count, total_freq
                                );
                                modified_line.insert_str(label_end, &freq_text);
                            }
                        }
                    }
                }
            }
            dot_string.push_str(&format!("    {}\n", modified_line.trim()));
        }
    }

    dot_string.push_str("}\n");
    Ok(dot_string)
}
