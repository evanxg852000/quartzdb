use std::path::Path;

use serde::de::DeserializeOwned;
use tokio::fs;

pub async fn read_as_object<T: DeserializeOwned>(file_path: &Path) -> anyhow::Result<T> {
    let file_extension = file_path
        .extension()
        .map(|os_str| os_str.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow::anyhow!("File must have extention to know the type"))?;

    let data = fs::read(file_path).await?;
    match file_extension.as_str() {
        "yaml" => serde_norway::from_slice::<T>(&data).map_err(|err| anyhow::anyhow!(err)),
        "json" => serde_json::from_slice::<T>(&data).map_err(|err| anyhow::anyhow!(err)),
        _ => Err(anyhow::anyhow!("Supported file formats are: `json, yamal`")),
    }
}
