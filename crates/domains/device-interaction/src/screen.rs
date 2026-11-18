//! One closed input contract shared by the tool catalog, Native adapter and
//! distributed protocol. Native-owned session and output paths are excluded.
use serde_json::{Map, Value, json};
use std::sync::OnceLock;
pub const MAX_INPUT_BYTES: usize = 32 * 1024;
const INPUT_SCHEMA: &str = include_str!("screen/input-v1.json");

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("screen input does not satisfy the closed device contract")]
pub struct ScreenInputError;
type Result<T> = std::result::Result<T, ScreenInputError>;
static VALIDATOR: OnceLock<Result<jsonschema::Validator>> = OnceLock::new();

pub fn input_schema() -> Result<Value> {
    serde_json::from_str(INPUT_SCHEMA).map_err(|_| ScreenInputError)
}
fn validator() -> Result<&'static jsonschema::Validator> {
    VALIDATOR
        .get_or_init(|| {
            jsonschema::draft202012::options()
                .build(&input_schema()?)
                .map_err(|_| ScreenInputError)
        })
        .as_ref()
        .map_err(|_| ScreenInputError)
}

#[derive(Clone, Debug)]
pub struct ScreenInput {
    tool: String,
    arguments: Map<String, Value>,
}
impl ScreenInput {
    pub fn from_json(value: &Value) -> Result<Self> {
        let bytes = serde_json::to_vec(value).map_err(|_| ScreenInputError)?;
        if bytes.len() > MAX_INPUT_BYTES || !validator()?.is_valid(value) {
            return Err(ScreenInputError);
        }
        let tool = value
            .get("tool")
            .and_then(Value::as_str)
            .ok_or(ScreenInputError)?
            .to_owned();
        let arguments = value
            .get("arguments")
            .and_then(Value::as_object)
            .ok_or(ScreenInputError)?
            .clone();
        Ok(Self { tool, arguments })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_INPUT_BYTES {
            return Err(ScreenInputError);
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| ScreenInputError)?;
        let input = Self::from_json(&value)?;
        if input.encode()? != bytes {
            return Err(ScreenInputError);
        }
        Ok(input)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.to_json()).map_err(|_| ScreenInputError)
    }
    pub fn to_json(&self) -> Value {
        json!({"tool":self.tool,"arguments":self.arguments})
    }
    pub fn tool(&self) -> &str {
        &self.tool
    }
    pub fn arguments(&self) -> &Map<String, Value> {
        &self.arguments
    }
    pub fn is_observation(&self) -> bool {
        matches!(
            self.tool.as_str(),
            "list_apps"
                | "list_windows"
                | "get_accessibility_tree"
                | "get_window_state"
                | "get_desktop_state"
                | "get_screen_size"
                | "get_cursor_position"
        )
    }
}

#[cfg(test)]
mod tests;
