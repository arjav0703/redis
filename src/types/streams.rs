use super::{RespHandler, RespValue};
use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>, // key-value pairs
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: Vec<StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub async fn add_entry(
        &mut self,
        id: String,
        fields: Vec<(String, String)>,
        handler: &mut RespHandler,
    ) -> (bool, String) {
        if id == "*" {
            let ms = chrono::Utc::now().timestamp_millis();
            let generated_id = format!("{ms}-0");
            self.entries.push(StreamEntry {
                id: generated_id.clone(),
                fields,
            });
            return (true, generated_id);
        }

        let id = self.auto_generate_id(&id);

        if let Err(e) = self.validate_id(&id) {
            let _ = handler
                .write_value(RespValue::SimpleError(format!("{e}")))
                .await;
            return (false, String::new());
        }
        self.entries.push(StreamEntry {
            id: id.clone(),
            fields,
        });
        (true, id)
    }

    fn auto_generate_id(&self, id: &str) -> String {
        let id_parts: Vec<&str> = id.split('-').collect();
        let seq = id_parts[1];

        let largest_id = self.get_largest_id().unwrap_or("0-0");
        let largest_parts: Vec<&str> = largest_id.split('-').collect();
        let largest_ms = largest_parts[0];
        let largest_seq = largest_parts[1];

        if seq == "*" {
            // If the millisecond part is the same as the largest ID, increment the sequence
            if id_parts[0] == largest_ms {
                if let Ok(largest_seq_num) = largest_seq.parse::<u64>() {
                    let id = format!("{}-{}", id_parts[0], largest_seq_num + 1);
                    dbg!(&id);
                    return id;
                }
            } else {
                // If the millisecond part is different, start sequence at 0
                let id = format!("{}-0", id_parts[0]);
                dbg!(&id);
                return id;
            }
        }

        // If no auto-generation is needed, return the original ID
        id.to_string()
    }

    #[allow(dead_code)]
    pub fn get_entries(&self) -> &[StreamEntry] {
        &self.entries
    }

    pub fn get_entries_after(&self, id: &str) -> Vec<&StreamEntry> {
        let mut result = Vec::new();

        for entry in &self.entries {
            if *entry.id > *id {
                result.push(entry);
            }
        }

        result
    }

    pub fn get_range(&self, start: &str, end: &str) -> Vec<&StreamEntry> {
        let mut result = Vec::new();

        for entry in &self.entries {
            if (start == "-" || *entry.id >= *start) && (end == "+" || *entry.id <= *end) {
                result.push(entry);
            }
        }

        result
    }

    fn validate_id(&self, id: &str) -> Result<(), anyhow::Error> {
        let id_parts: Vec<&str> = id.split('-').collect();

        let id_ms: u64 = id_parts[0]
            .parse()
            .map_err(|_| anyhow!("Invalid ID format"))?;
        let id_seq: u64 = id_parts[1]
            .parse()
            .map_err(|_| anyhow!("Invalid ID format"))?;

        // Check if ID is 0-0
        if id_ms == 0 && id_seq == 0 {
            return Err(anyhow!(
                "ERR The ID specified in XADD must be greater than 0-0"
            ));
        }

        let largest_id = self.get_largest_id().unwrap_or("0-0");
        dbg!(&largest_id);
        let largest_parts: Vec<&str> = largest_id.split('-').collect();
        let largest_ms: u64 = largest_parts[0].parse().unwrap_or(0);
        let largest_seq: u64 = largest_parts[1].parse().unwrap_or(0);

        if id_ms < largest_ms {
            return Err(anyhow!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            ));
        }

        if id_ms == largest_ms && id_seq <= largest_seq {
            return Err(anyhow!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            ));
        }

        Ok(())
    }

    pub fn get_largest_id(&self) -> Option<&str> {
        self.entries.last().map(|entry| entry.id.as_str())
    }
}

impl StreamEntry {
    #[allow(dead_code)]
    pub fn get_id(&self) -> &str {
        &self.id
    }

    #[allow(dead_code)]
    pub fn get_fields(&self) -> &[(String, String)] {
        &self.fields
    }
}
