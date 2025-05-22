use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metrics {
    pub total_records: i64,
    pub total_files_size_bytes: i64,
    pub total_data_files: i64,
    pub total_delete_files: i64,
    pub total_position_deletes: i64,
    pub total_equality_deletes: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_serialization_deserialization() {
        let original_metrics = Metrics {
            total_records: 1000,
            total_files_size_bytes: 1024000,
            total_data_files: 10,
            total_delete_files: 2,
            total_position_deletes: 50,
            total_equality_deletes: 5,
        };

        // Serialize to JSON
        let json_string = serde_json::to_string(&original_metrics).expect("Serialization failed");

        // Deserialize from JSON
        let deserialized_metrics: Metrics =
            serde_json::from_str(&json_string).expect("Deserialization failed");

        // Assert equality
        assert_eq!(original_metrics, deserialized_metrics);

        // Test with a known valid JSON string
        let valid_json = r#"
        {
            "total_records": 2000,
            "total_files_size_bytes": 2048000,
            "total_data_files": 20,
            "total_delete_files": 4,
            "total_position_deletes": 100,
            "total_equality_deletes": 10
        }
        "#;
        let metrics_from_valid_json: Metrics =
            serde_json::from_str(valid_json).expect("Deserialization of valid JSON failed");
        assert_eq!(metrics_from_valid_json.total_records, 2000);

        // Test with missing fields (should fail as no default attribute is set)
        let json_missing_fields = r#"
        {
            "total_records": 1000
        }
        "#;
        let result_missing: Result<Metrics, _> = serde_json::from_str(json_missing_fields);
        assert!(
            result_missing.is_err(),
            "Deserialization should fail for missing fields"
        );

        // Test with incorrect type (should fail)
        let json_incorrect_type = r#"
        {
            "total_records": "not_a_number",
            "total_files_size_bytes": 1024000,
            "total_data_files": 10,
            "total_delete_files": 2,
            "total_position_deletes": 50,
            "total_equality_deletes": 5
        }
        "#;
        let result_incorrect_type: Result<Metrics, _> = serde_json::from_str(json_incorrect_type);
        assert!(
            result_incorrect_type.is_err(),
            "Deserialization should fail for incorrect type"
        );
    }
}
