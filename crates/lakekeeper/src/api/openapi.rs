use utoipa::{
    PartialSchema, ToSchema,
    openapi::{Content, OpenApi, Ref, response::ResponseBuilder},
};

use super::IcebergErrorResponse;

/// Fills in the missing client and server error ranges in an assembled API document.
pub(super) fn add_iceberg_error_responses(openapi: &mut OpenApi) {
    let components = openapi.components.get_or_insert_default();
    let schema_name = IcebergErrorResponse::name();
    let content = Content::new(Some(Ref::from_schema_name(schema_name.as_ref())));
    let mut error_schemas = vec![(schema_name.into_owned(), IcebergErrorResponse::schema())];
    IcebergErrorResponse::schemas(&mut error_schemas);
    for (name, schema) in error_schemas {
        components.schemas.entry(name).or_insert(schema);
    }

    for (status, name, description) in [
        (
            "4XX",
            "IcebergClientError",
            "Client error. Request rejections may have a plain text or empty body.",
        ),
        (
            "5XX",
            "IcebergServerError",
            "Server error. Failures outside the handler may have a plain text or empty body.",
        ),
    ] {
        components
            .responses
            .entry(name.to_owned())
            .or_insert_with(|| {
                ResponseBuilder::new()
                    .description(description)
                    .content("application/json", content.clone())
                    .into()
            });
        let response = Ref::from_response_name(name);
        for path in openapi.paths.paths.values_mut() {
            // HEAD responses have no body.
            for operation in [
                &mut path.get,
                &mut path.put,
                &mut path.post,
                &mut path.delete,
                &mut path.options,
                &mut path.patch,
                &mut path.trace,
            ]
            .into_iter()
            .flatten()
            {
                operation
                    .responses
                    .responses
                    .entry(status.to_owned())
                    .or_insert_with(|| response.clone().into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn adds_error_ranges_without_replacing_explicit_responses() {
        let mut doc: OpenApi = serde_json::from_value(json!({
            "openapi": "3.1.0",
            "info": {"title": "Test API", "version": "0.1.0"},
            "paths": {
                "/table": {
                    "get": {"responses": {
                        "200": {"description": "Table"},
                        "409": {"description": "Table is locked"},
                        "4XX": {"description": "Table request failed"}
                    }},
                    "post": {"responses": {"204": {"description": "Created"}}},
                    "head": {"responses": {"404": {"description": "Not found"}}}
                }
            }
        }))
        .unwrap();

        add_iceberg_error_responses(&mut doc);

        let doc = serde_json::to_value(doc).unwrap();
        let client = json!({"$ref": "#/components/responses/IcebergClientError"});
        let server = json!({"$ref": "#/components/responses/IcebergServerError"});
        assert_eq!(
            doc["paths"]["/table"]["get"]["responses"],
            json!({
                "200": {"description": "Table"},
                "409": {"description": "Table is locked"},
                "4XX": {"description": "Table request failed"},
                "5XX": server,
            })
        );
        assert_eq!(
            doc["paths"]["/table"]["post"]["responses"],
            json!({
                "204": {"description": "Created"},
                "4XX": client,
                "5XX": server,
            })
        );
        assert_eq!(
            doc["paths"]["/table"]["head"]["responses"],
            json!({"404": {"description": "Not found"}})
        );
        let components = &doc["components"];
        for name in ["IcebergClientError", "IcebergServerError"] {
            assert_eq!(
                components["responses"][name]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/IcebergErrorResponse"
            );
        }
        assert_eq!(
            components["schemas"]["IcebergErrorResponse"]["properties"]["error"]["$ref"],
            "#/components/schemas/ErrorModel"
        );
        assert!(components["schemas"]["ErrorModel"].is_object());
    }

    #[test]
    fn rest_catalog_error_responses_match_the_iceberg_envelope() {
        let spec = include_str!("../../../../docs/docs/api/rest-catalog-open-api.yaml");
        let doc: serde_json::Value = serde_norway::from_str(spec).unwrap();
        for (uri, path) in doc["paths"].as_object().unwrap() {
            if uri == "/v1/oauth/tokens" {
                // OAuth uses its own error schema; Lakekeeper does not serve this route.
                continue;
            }
            for (method, op) in path.as_object().unwrap() {
                let Some(responses) = op["responses"].as_object() else {
                    continue;
                };
                assert!(responses.contains_key("4XX"), "{method} {uri}");
                assert!(responses.contains_key("5XX"), "{method} {uri}");
                for (status, response) in responses {
                    if !status.starts_with(['4', '5']) {
                        continue;
                    }
                    let response = response["$ref"].as_str().map_or(response, |reference| {
                        doc.pointer(reference.strip_prefix('#').unwrap()).unwrap()
                    });
                    if method == "head" {
                        assert!(response.get("content").is_none(), "HEAD {uri} {status}");
                    } else if let Some(json) = response["content"].get("application/json") {
                        assert_eq!(
                            json["schema"]["$ref"], "#/components/schemas/IcebergErrorResponse",
                            "{method} {uri} {status}"
                        );
                    }
                }
            }
        }
    }
}
