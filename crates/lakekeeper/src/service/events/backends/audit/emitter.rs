//! Who emits audit records.
//!
//! An emitter is a type, one per product, declared once with [`crate::declare_audit_emitter!`] and
//! re-exported by every crate of that product as `crate::audit_emitter`. Because it is a type,
//! "same emitter" is a compile-time fact: a record cannot mix the vocabulary of two emitters.

/// One product that emits audit records, at one version of its own vocabulary and context
/// shapes.
pub trait AuditEmitter: 'static {
    /// Lowercase letters, digits and `-`, starting with a letter. `lakekeeper`, `lakekeeper-plus`.
    const NAME: &'static str;
    /// `MAJOR.MINOR`. Derived from the emitter's fragments by the checker, never edited by hand.
    const FORMAT: &'static str;
}

/// Whether `s` is a valid emitter name: non-empty, lowercase ASCII letters, digits and `-`,
/// starting with a letter.
#[must_use]
pub const fn is_emitter_name(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() || !b[0].is_ascii_lowercase() {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        if !(b[i].is_ascii_lowercase() || b[i].is_ascii_digit() || b[i] == b'-') {
            return false;
        }
        i += 1;
    }
    true
}

/// Declares the audit emitter of a product.
///
/// Writes the unit struct, its [`AuditEmitter`] impl, compile-time assertions on the name and
/// the format, and the `audit_emitter` module every audit type in the crate is bound to.
/// Invoke it once, at the root of the crate that owns the product's audit format; every other
/// crate of the product re-exports the module:
/// `pub mod audit_emitter { pub use <that crate>::audit_emitter::*; }`.
#[macro_export]
macro_rules! declare_audit_emitter {
    ($ty:ident, name = $name:literal, format = $format:expr $(,)?) => {
        /// The audit emitter of this product.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $ty;

        impl $crate::audit::AuditEmitter for $ty {
            const NAME: &'static str = $name;
            const FORMAT: &'static str = $format;
        }

        const _: () = {
            assert!(
                $crate::audit::is_emitter_name(<$ty as $crate::audit::AuditEmitter>::NAME),
                "emitter name: lowercase letters, digits and '-', starting with a letter"
            );
            assert!(
                $crate::audit::is_major_minor(<$ty as $crate::audit::AuditEmitter>::FORMAT),
                "emitter format must be MAJOR.MINOR"
            );
        };

        /// The emitter every audit type in this crate belongs to.
        pub mod audit_emitter {
            /// The product's emitter type.
            pub type Emitter = super::$ty;
        }
    };
}
