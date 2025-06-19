// from lib/src/components/logical.rs

// ... inside AndConstraintComponent::validate ...
                    ConformanceReport::NonConforms(failure) => {
                        // value_node_to_check DOES NOT CONFORM to this conjunct_node_shape.
                        // For sh:and, all shapes must conform. So, this is a failure for this value_node.
                        let mut error_context = c.clone();
                        error_context.with_value(value_node_to_check.clone());
                        let message = format!(
                            "Value {:?} does not conform to sh:and shape {:?}: {}",
                            value_node_to_check, conjunct_shape_id, failure.message
                        );
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node_to_check.clone()),
                            message,
                        };
                        results.push(ComponentValidationResult::Fail(error_context, failure));
                        break; // Fails one, fails all for this value node.
                    }
// ...
