use std::{
    fmt::{Display, Formatter},
    ops::Deref,
};

use iceberg::TableIdent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::generic::{GenericTableId, TableId, ViewId};

#[derive(Hash, PartialOrd, PartialEq, Debug, Clone, Copy, Eq, Deserialize, Serialize)]
#[serde(tag = "type", content = "id", rename_all = "kebab-case")]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "open-api", schema(as=TabularIdentUuid))]
pub enum TabularId {
    #[cfg_attr(feature = "open-api", schema(value_type = Uuid))]
    Table(TableId),
    #[cfg_attr(feature = "open-api", schema(value_type = Uuid))]
    View(ViewId),
    #[cfg_attr(feature = "open-api", schema(value_type = Uuid))]
    GenericTable(GenericTableId),
    #[cfg_attr(feature = "open-api", schema(value_type = Uuid))]
    PaimonTable(TableId),
}

impl TabularId {
    #[must_use]
    pub fn typ_str(&self) -> &'static str {
        match self {
            TabularId::Table(_) => "Table",
            TabularId::View(_) => "View",
            TabularId::GenericTable(_) => "GenericTable",
            TabularId::PaimonTable(_) => "PaimonTable",
        }
    }

    #[must_use]
    pub fn is_table(&self) -> bool {
        matches!(self, TabularId::Table(_))
    }

    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(self, TabularId::View(_))
    }

    #[must_use]
    pub fn is_generic_table(&self) -> bool {
        matches!(self, TabularId::GenericTable(_))
    }

    #[must_use]
    pub fn is_paimon_table(&self) -> bool {
        matches!(self, TabularId::PaimonTable(_))
    }
}

impl AsRef<Uuid> for TabularId {
    fn as_ref(&self) -> &Uuid {
        match self {
            TabularId::Table(id) => id.as_ref(),
            TabularId::View(id) => id.as_ref(),
            TabularId::GenericTable(id) => id.as_ref(),
            TabularId::PaimonTable(id) => id.as_ref(),
        }
    }
}

impl Display for TabularId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &**self)
    }
}

impl From<TableId> for TabularId {
    fn from(value: TableId) -> Self {
        Self::Table(value)
    }
}

impl From<ViewId> for TabularId {
    fn from(value: ViewId) -> Self {
        Self::View(value)
    }
}

impl From<GenericTableId> for TabularId {
    fn from(value: GenericTableId) -> Self {
        Self::GenericTable(value)
    }
}

// We get these two types since we are using them as HashMap keys. Those need to be sized,
// implementing these types via Cow makes them not sized, so we go for two... not ideal.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TabularIdentBorrowed<'a> {
    Table(&'a TableIdent),
    #[allow(dead_code)]
    View(&'a TableIdent),
    #[allow(dead_code)]
    GenericTable(&'a TableIdent),
    #[allow(dead_code)]
    PaimonTable(&'a TableIdent),
}

impl TabularIdentBorrowed<'_> {
    #[must_use]
    pub fn typ_str(&self) -> &'static str {
        match self {
            TabularIdentBorrowed::Table(_) => "Table",
            TabularIdentBorrowed::View(_) => "View",
            TabularIdentBorrowed::GenericTable(_) => "GenericTable",
            TabularIdentBorrowed::PaimonTable(_) => "PaimonTable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TabularIdentOwned {
    Table(TableIdent),
    View(TableIdent),
    GenericTable(TableIdent),
    PaimonTable(TableIdent),
}

impl TabularIdentOwned {
    #[must_use]
    pub fn into_inner(self) -> TableIdent {
        match self {
            TabularIdentOwned::Table(ident)
            | TabularIdentOwned::View(ident)
            | TabularIdentOwned::GenericTable(ident)
            | TabularIdentOwned::PaimonTable(ident) => ident,
        }
    }

    #[must_use]
    pub fn as_borrowed(&self) -> TabularIdentBorrowed<'_> {
        match self {
            TabularIdentOwned::Table(ident) => TabularIdentBorrowed::Table(ident),
            TabularIdentOwned::View(ident) => TabularIdentBorrowed::View(ident),
            TabularIdentOwned::GenericTable(ident) => TabularIdentBorrowed::GenericTable(ident),
            TabularIdentOwned::PaimonTable(ident) => TabularIdentBorrowed::PaimonTable(ident),
        }
    }

    #[must_use]
    pub fn as_table_ident(&self) -> &TableIdent {
        match self {
            TabularIdentOwned::Table(ident)
            | TabularIdentOwned::View(ident)
            | TabularIdentOwned::GenericTable(ident)
            | TabularIdentOwned::PaimonTable(ident) => ident,
        }
    }
}

impl<'a> From<TabularIdentBorrowed<'a>> for TabularIdentOwned {
    fn from(ident: TabularIdentBorrowed<'a>) -> Self {
        match ident {
            TabularIdentBorrowed::Table(ident) => TabularIdentOwned::Table(ident.clone()),
            TabularIdentBorrowed::View(ident) => TabularIdentOwned::View(ident.clone()),
            TabularIdentBorrowed::GenericTable(ident) => {
                TabularIdentOwned::GenericTable(ident.clone())
            }
            TabularIdentBorrowed::PaimonTable(ident) => {
                TabularIdentOwned::PaimonTable(ident.clone())
            }
        }
    }
}

impl TabularIdentBorrowed<'_> {
    #[must_use]
    pub fn as_table_ident(&self) -> &TableIdent {
        match self {
            TabularIdentBorrowed::Table(ident)
            | TabularIdentBorrowed::View(ident)
            | TabularIdentBorrowed::GenericTable(ident)
            | TabularIdentBorrowed::PaimonTable(ident) => ident,
        }
    }
}

impl Deref for TabularId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        match self {
            TabularId::Table(id) => id.as_ref(),
            TabularId::View(id) => id.as_ref(),
            TabularId::GenericTable(id) => id.as_ref(),
            TabularId::PaimonTable(id) => id.as_ref(),
        }
    }
}
