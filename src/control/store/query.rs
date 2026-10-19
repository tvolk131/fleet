//! Bounded, linearizable-at-each-page reads. Cursors are live keyset walks, not frozen exports.
use super::*;
use indexed::{Collection, RECEIPTS, ROWS};
use std::ops::Bound::{Excluded, Included, Unbounded};

const PAGE_BYTES: usize = 2 << 20;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cursor {
    pub cluster: String,
    pub collection: Option<Collection>,
    pub after: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Query {
    Job {
        id: String,
    },
    View,
    Header,
    Stats,
    Get {
        collection: Collection,
        key: String,
    },
    Receipt {
        principal: String,
        request_id: String,
    },
    Page {
        collection: Option<Collection>,
        cursor: Option<Cursor>,
        limit: u16,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub cluster: String,
    pub applied: Option<LogId<u64>>,
    pub items: Vec<(String, serde_json::Value)>,
    pub next: Option<Cursor>,
}

impl Store {
    pub async fn query(&self, query: Query) -> Result<serde_json::Value, StorageError<u64>> {
        self.blocking(false, move |db| {
            let tx = db.begin_read()?;
            let table = tx.open_table(META)?;
            let header: State = read(&table, "state")?.context("missing state")?;
            let (format, _, _): (u32, String, u64) = read(&table, "identity")?.context("missing identity")?;
            match query {
                Query::Job { id } => {
                    let state = indexed::read_state(&tx, false)?;
                    Ok(super::super::client::job_view(&state, state.jobs.get(&id).context("unknown job")?))
                }
                Query::View => Ok(serde_json::to_value(indexed::read_state(&tx, false)?)?),
                Query::Header => {
                    let mut value = serde_json::to_value(header)?;
                    value["receipts"] = serde_json::json!({});
                    for collection in indexed::COLLECTIONS {
                        *value.pointer_mut(collection.path()).context("missing resource map")? = serde_json::json!({});
                    }
                    Ok(value)
                }
                Query::Stats => {
                    let receipts = tx.open_table(RECEIPTS)?.len()?;
                    let resources = tx.open_table(ROWS)?.len()?;
                    Ok(serde_json::json!({"cluster":header.cluster,"applied":header.last_applied,"physical_format":format,"indexed":true,"receipts":receipts,"resource_rows":resources,"receipt_retention":"indefinite"}))
                }
                Query::Get { collection, key } => {
                    Ok(tx.open_table(ROWS)?.get((collection as u8, key.as_str()))?
                            .map(|r| serde_json::from_slice(r.value())).transpose()?.unwrap_or(serde_json::Value::Null))
                }
                Query::Receipt { principal, request_id } => {
                    let key = super::super::model::key(&principal, &request_id);
                    Ok(read::<serde_json::Value>(&tx.open_table(RECEIPTS)?, &key)?.unwrap_or(serde_json::Value::Null))
                }
                Query::Page { collection, cursor, limit } => {
                    ensure!((1..=256).contains(&limit), "page limit must be 1..=256");
                    if let Some(c) = &cursor { ensure!(c.cluster == header.cluster && c.collection == collection, "cursor cluster or collection mismatch"); }
                    let mut page = Page { cluster: header.cluster.clone(), applied: header.last_applied, items: vec![], next: None };
                    let mut bytes = 0;
                    let mut append = |key: &str, value: &[u8]| -> Result<bool> {
                        ensure!(value.len() + key.len() <= PAGE_BYTES, "individual record exceeds page budget; use a point query or offline export");
                        if page.items.len() == limit as usize || bytes + value.len() + key.len() > PAGE_BYTES {
                            page.next = Some(Cursor { cluster: header.cluster.clone(), collection, after: page.items.last().context("empty page")?.0.clone() });
                            return Ok(false);
                        }
                        bytes += value.len() + key.len();
                        page.items.push((key.into(), serde_json::from_slice(value)?));
                        Ok(true)
                    };
                    {
                        if let Some(collection) = collection {
                            let start = cursor.as_ref().map_or(Included((collection as u8, "")), |c| Excluded((collection as u8, c.after.as_str())));
                            for row in tx.open_table(ROWS)?.range((start, Excluded((collection as u8 + 1, ""))))? {
                                let (key, value) = row?;
                                if !append(key.value().1, value.value())? { break; }
                            }
                        } else {
                            let start = cursor.as_ref().map_or(Unbounded, |c| Excluded(c.after.as_str()));
                            for row in tx.open_table(RECEIPTS)?.range::<&str>((start, Unbounded))? {
                                let (key, value) = row?;
                                if !append(key.value(), value.value())? { break; }
                            }
                        }
                    }
                    Ok(serde_json::to_value(page)?)
                }
            }
        }).await
    }
}
