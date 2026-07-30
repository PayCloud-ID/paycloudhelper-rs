use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    oracle: String,
    captured_at: String,
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    name: String,
    input: String,
    output: String,
}

#[test]
fn go_json_compact_vectors_are_byte_exact() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("vectors/json_minify.json")).expect("valid fixture");
    assert!(fixture.oracle.contains("encoding/json.Compact"));
    assert_eq!(fixture.captured_at, "2026-07-30");

    for vector in fixture.vectors {
        let actual = pc_core::json_minify(vector.input.as_bytes())
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(actual, vector.output.as_bytes(), "{}", vector.name);
    }
}
