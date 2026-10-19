//! The replicated state machine remains the semantic authority. This is its physical layout:
//! a small header, keyed resource rows, and permanent request receipts, committed together.
use super::*;
use std::collections::BTreeMap;

pub const FORMAT: u32 = super::super::FORMAT;
pub(super) const ROWS: TableDefinition<(u8, &str), &[u8]> = TableDefinition::new("state_rows");
pub(super) const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("request_receipts");

/// Discriminants are durable schema identifiers. Never renumber or reuse one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Collection {
    Jobs = 1,
    Assignments = 2,
    Checkpoints = 3,
    Workers = 4,
    MembershipOperations = 5,
    Cursors = 6,
    VmOperations = 7,
    Artifacts = 8,
    HistoryAttempts = 9,
    HistoryJobs = 10,
}
pub(super) const COLLECTIONS: [Collection; 10] = [
    Collection::Jobs,
    Collection::Assignments,
    Collection::Checkpoints,
    Collection::Workers,
    Collection::MembershipOperations,
    Collection::Cursors,
    Collection::VmOperations,
    Collection::Artifacts,
    Collection::HistoryAttempts,
    Collection::HistoryJobs,
];
impl Collection {
    pub(super) fn path(self) -> &'static str {
        match self {
            Self::Jobs => "/jobs",
            Self::Assignments => "/assignments",
            Self::Checkpoints => "/checkpoints",
            Self::Workers => "/workers",
            Self::MembershipOperations => "/membership_operations",
            Self::Cursors => "/cursors",
            Self::VmOperations => "/vm/operations",
            Self::Artifacts => "/storage/artifacts",
            Self::HistoryAttempts => "/history/attempts",
            Self::HistoryJobs => "/history/jobs",
        }
    }
}

/// Resource hydration is needed by the existing cross-resource invariants, but receipt history
/// is deliberately excluded. Targeted queries use the rows directly without hydration.
pub(super) fn hydrate(
    header: State,
    rows: &impl ReadableTable<(u8, &'static str), &'static [u8]>,
) -> Result<State> {
    let mut value = serde_json::to_value(header)?;
    for collection in COLLECTIONS {
        let target = value
            .pointer_mut(collection.path())
            .context("missing resource map")?
            .as_object_mut()
            .context("invalid resource map")?;
        for row in rows.range((collection as u8, "")..(collection as u8 + 1, ""))? {
            let (key, data) = row?;
            target.insert(key.value().1.into(), serde_json::from_slice(data.value())?);
        }
    }
    Ok(serde_json::from_value(value)?)
}

/// Only changed resource rows are written; an unrelated request never rewrites receipt history.
/// Called inside the very same redb transaction as last_applied and receipts.
pub(super) fn persist(tx: &redb::WriteTransaction, state: &State) -> Result<()> {
    let mut value = serde_json::to_value(state)?;
    let mut rows = tx.open_table(ROWS)?;
    for collection in COLLECTIONS {
        let map = value
            .pointer_mut(collection.path())
            .context("missing resource map")?
            .as_object_mut()
            .context("invalid resource map")?;
        let existing: Vec<String> = rows
            .range((collection as u8, "")..(collection as u8 + 1, ""))?
            .map(|r| r.map(|(k, _)| k.value().1.to_string()))
            .collect::<std::result::Result<_, _>>()?;
        for key in existing {
            if !map.contains_key(&key) {
                rows.remove((collection as u8, key.as_str()))?;
            }
        }
        for (key, entry) in map.iter() {
            let bytes = serde_json::to_vec(entry)?;
            let changed = rows
                .get((collection as u8, key.as_str()))?
                .is_none_or(|old| old.value() != bytes);
            if changed {
                rows.insert((collection as u8, key.as_str()), bytes.as_slice())?;
            }
        }
        map.clear();
    }
    value["receipts"] = serde_json::json!({});
    // Keep the semantic State format stable; the identity marks the physical representation.
    put(&mut tx.open_table(META)?, "state", &value)?;
    let mut receipts = tx.open_table(RECEIPTS)?;
    for (key, receipt) in &state.receipts {
        put(&mut receipts, key, receipt)?;
    }
    Ok(())
}

pub(super) fn read_state(tx: &redb::ReadTransaction, history: bool) -> Result<State> {
    let table = tx.open_table(META)?;
    let mut state: State = read(&table, "state")?.context("missing state")?;
    let identity: (u32, String, u64) = read(&table, "identity")?.context("missing identity")?;
    ensure!(
        identity.0 == FORMAT && state.family == super::super::FAMILY,
        "incompatible metadata family"
    );
    {
        state = hydrate(state, &tx.open_table(ROWS)?)?;
        if history {
            state.receipts = tx
                .open_table(RECEIPTS)?
                .iter()?
                .map(|row| {
                    let (key, value) = row?;
                    Ok((
                        key.value().to_owned(),
                        serde_json::from_slice(value.value())?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
        }
    }
    Ok(state)
}

pub(super) fn load_receipts(
    state: &mut State,
    command: &super::super::Command,
    table: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<()> {
    state.receipts.clear();
    let mut keys = vec![super::super::model::key(
        &command.principal,
        &command.request.id,
    )];
    if let super::super::Mutation::Release { request_id } = &command.request.mutation {
        keys.push(super::super::model::key(&command.principal, request_id));
    }
    for key in keys {
        if let Some(receipt) = read(table, &key)? {
            state.receipts.insert(key, receipt);
        }
    }
    Ok(())
}
