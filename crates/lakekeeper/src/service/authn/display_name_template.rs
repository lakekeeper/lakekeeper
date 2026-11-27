//! A display name built from a token's claims, for tokens that carry no name claim.

/// A structurally invalid display-name template — one that can never render for
/// any token, independent of its claims. (A well-formed template whose claim is
/// merely absent from a given token is *not* an error; it simply doesn't apply
/// to that token.) Returned by [`DisplayNameTemplate::parse`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DisplayNameTemplateError {
    /// A `{` with no matching `}` (write a literal brace as `{{`).
    #[error("unmatched `{{` in display-name template (write a literal brace as `{{{{`)")]
    UnmatchedOpenBrace,
    /// A `}` with no matching `{` (write a literal brace as `}}`).
    #[error("unmatched `}}` in display-name template (write a literal brace as `}}}}`)")]
    UnmatchedCloseBrace,
    /// An empty `{}` placeholder — a placeholder must name a claim.
    #[error("empty `{{}}` placeholder in display-name template (name a claim, e.g. `{{email}}`)")]
    EmptyPlaceholder,
}

/// A validated display-name template: a sequence of literal text and
/// `{claim.path}` placeholders. Constructed via [`DisplayNameTemplate::parse`],
/// so holding one is proof its syntax is valid. Resolved against a token's
/// claims when the token carries no name claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisplayNameTemplate {
    source: String,
    segments: Vec<Segment>,
}

/// One piece of a parsed [`DisplayNameTemplate`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// Verbatim text, with any `{{`/`}}` already unescaped to a single brace.
    Literal(String),
    /// A claim path (dot-notation) to resolve at render time.
    Claim(String),
}

impl DisplayNameTemplate {
    /// Parse and validate a display-name template.
    ///
    /// Placeholders are `{claim.path}` (dot-notation); write a literal brace by
    /// doubling it (`{{`/`}}`), as in [`std::fmt`]. The *structure* is validated
    /// here — braces must balance and every placeholder must name a claim — but
    /// whether a referenced claim exists is token-dependent and is checked only at
    /// render time.
    ///
    /// # Errors
    /// Returns [`DisplayNameTemplateError`] for an unmatched `{`, a stray `}`, or
    /// an empty `{}` placeholder.
    pub(crate) fn parse(template: &str) -> Result<Self, DisplayNameTemplateError> {
        let mut segments = Vec::new();
        let mut literal = String::new();
        let mut rest = template;
        while let Some(idx) = rest.find(['{', '}']) {
            literal.push_str(&rest[..idx]);
            let brace = rest.as_bytes()[idx];
            rest = &rest[idx + 1..];

            if brace == b'}' {
                // A `}` is only valid as the escape `}}`; a lone `}` is malformed.
                rest = rest
                    .strip_prefix('}')
                    .ok_or(DisplayNameTemplateError::UnmatchedCloseBrace)?;
                literal.push('}');
                continue;
            }

            // `brace == b'{'`: either the escape `{{` or the start of a placeholder.
            if let Some(stripped) = rest.strip_prefix('{') {
                literal.push('{');
                rest = stripped;
                continue;
            }
            let close = rest
                .find('}')
                .ok_or(DisplayNameTemplateError::UnmatchedOpenBrace)?;
            let path = &rest[..close];
            // A `{` before the closing `}` means an earlier `{` was never closed.
            if path.contains('{') {
                return Err(DisplayNameTemplateError::UnmatchedOpenBrace);
            }
            // Empty or whitespace-only placeholders name no claim.
            if path.trim().is_empty() {
                return Err(DisplayNameTemplateError::EmptyPlaceholder);
            }
            if !literal.is_empty() {
                segments.push(Segment::Literal(std::mem::take(&mut literal)));
            }
            segments.push(Segment::Claim(path.to_string()));
            rest = &rest[close + 1..];
        }
        literal.push_str(rest);
        if !literal.is_empty() {
            segments.push(Segment::Literal(literal));
        }
        Ok(Self {
            source: template.to_string(),
            segments,
        })
    }

    /// Render the template against `claims`. Returns `None` — so the caller keeps
    /// its own default — when a referenced claim is missing or not a string
    /// (logged at `debug`), or when the result is empty or all-whitespace.
    /// Surrounding whitespace is trimmed.
    #[cfg(any(feature = "router", feature = "test-utils", test))]
    pub(crate) fn render(&self, claims: &limes::VerifiedClaims) -> Option<String> {
        let mut out = String::with_capacity(self.source.len());
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => out.push_str(text),
                Segment::Claim(path) => {
                    let Some(value) = claims.claim(path).and_then(serde_json::Value::as_str) else {
                        tracing::debug!(
                            "Display name template `{}` not applied: claim `{path}` is missing or not a string.",
                            self.source
                        );
                        return None;
                    };
                    out.push_str(value);
                }
            }
        }
        let trimmed = out.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use limes::VerifiedClaims;

    use super::*;

    fn parse_ok(template: &str) -> DisplayNameTemplate {
        DisplayNameTemplate::parse(template).expect("template should be valid")
    }

    fn claims(claims: serde_json::Value) -> VerifiedClaims {
        VerifiedClaims::try_from(claims).unwrap()
    }

    #[test]
    fn test_display_name_template_parse_accepts_well_formed() {
        let segments = |template: &str| parse_ok(template).segments;
        let literal = |text: &str| Segment::Literal(text.to_string());
        let claim = |path: &str| Segment::Claim(path.to_string());
        assert_eq!(
            segments("Service Account {email}"),
            [literal("Service Account "), claim("email")]
        );
        assert_eq!(segments("{sub}"), [claim("sub")]);
        assert_eq!(
            segments("literal, no placeholders"),
            [literal("literal, no placeholders")]
        );
        assert_eq!(
            segments("{{escaped}} {sub}"),
            [literal("{escaped} "), claim("sub")]
        );
        // Claim existence is token-dependent and deliberately NOT checked at parse.
        assert_eq!(segments("{a.b.c}"), [claim("a.b.c")]);
    }

    #[test]
    fn test_display_name_template_parse_rejects_structural_errors() {
        assert_eq!(
            DisplayNameTemplate::parse("Team {Alpha"),
            Err(DisplayNameTemplateError::UnmatchedOpenBrace)
        );
        assert_eq!(
            DisplayNameTemplate::parse("Team Alpha}"),
            Err(DisplayNameTemplateError::UnmatchedCloseBrace)
        );
        assert_eq!(
            DisplayNameTemplate::parse("{}"),
            Err(DisplayNameTemplateError::EmptyPlaceholder)
        );
        // A `{` inside a placeholder means an earlier `{` was never closed.
        assert_eq!(
            DisplayNameTemplate::parse("{a{b}"),
            Err(DisplayNameTemplateError::UnmatchedOpenBrace)
        );
        // A whitespace-only placeholder names no claim.
        assert_eq!(
            DisplayNameTemplate::parse("{ }"),
            Err(DisplayNameTemplateError::EmptyPlaceholder)
        );
        assert_eq!(
            DisplayNameTemplate::parse("{   }"),
            Err(DisplayNameTemplateError::EmptyPlaceholder)
        );
    }

    #[test]
    fn test_render_substitutes_email() {
        let claims = claims(serde_json::json!({ "email": "sa@example.com", "sub": "abc" }));
        assert_eq!(
            parse_ok("Service Account {email}").render(&claims),
            Some("Service Account sa@example.com".to_string())
        );
    }

    #[test]
    fn test_render_substitutes_sub() {
        let claims = claims(serde_json::json!({ "sub": "abc-123" }));
        assert_eq!(
            parse_ok("{sub}").render(&claims),
            Some("abc-123".to_string())
        );
    }

    #[test]
    fn test_render_navigates_nested_claim() {
        let claims = claims(serde_json::json!({ "a": { "b": { "c": "deep" } } }));
        assert_eq!(
            parse_ok("name={a.b.c}").render(&claims),
            Some("name=deep".to_string())
        );
    }

    #[test]
    fn test_render_escaped_braces() {
        let claims = claims(serde_json::json!({ "sub": "abc" }));
        // `{{`/`}}` are literal braces, matching `format!` semantics.
        assert_eq!(parse_ok("{{}}").render(&claims), Some("{}".to_string()));
        assert_eq!(
            parse_ok("{{{sub}}}").render(&claims),
            Some("{abc}".to_string())
        );
        assert_eq!(
            parse_ok("Team {{Alpha}}").render(&claims),
            Some("Team {Alpha}".to_string())
        );
    }

    #[test]
    fn test_render_literal_only() {
        let claims = claims(serde_json::json!({ "sub": "abc" }));
        assert_eq!(
            parse_ok("Machine Account").render(&claims),
            Some("Machine Account".to_string())
        );
    }

    #[test]
    fn test_render_missing_claim_yields_none() {
        // `email` is absent, so the template does not apply and the caller falls
        // back to its own default; a half-filled string is never rendered.
        let claims = claims(serde_json::json!({ "sub": "abc" }));
        assert_eq!(parse_ok("Service Account {email}").render(&claims), None);
    }

    #[test]
    fn test_render_non_string_claim_yields_none() {
        // A claim that exists but is not a string (e.g. an array) does not apply.
        let claims = claims(serde_json::json!({ "organizations": ["org-1"] }));
        assert_eq!(parse_ok("{organizations}").render(&claims), None);
    }

    #[test]
    fn test_render_does_not_rescan_substituted_value() {
        // A claim value that itself contains braces must never be re-interpreted
        // as a placeholder — render walks pre-parsed segments, not the output.
        let claims = claims(serde_json::json!({ "org": "Team {X}" }));
        assert_eq!(
            parse_ok("{org}").render(&claims),
            Some("Team {X}".to_string())
        );
    }

    #[test]
    fn test_render_trims_surrounding_whitespace() {
        let claims = claims(serde_json::json!({ "sub": "abc" }));
        assert_eq!(
            parse_ok("  {sub}  ").render(&claims),
            Some("abc".to_string())
        );
    }
}
