use shacl::context::ValidationContext;

fn main() {
    let ctx = ValidationContext::from_files("Brick.ttl", "air_quality_sensor_example.ttl").unwrap();
    ctx.graphviz();
}
