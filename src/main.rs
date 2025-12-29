use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use http::StatusCode;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio;
use tokio::net::TcpListener;
use std::fs::File;


#[derive(Debug, Clone)]
pub struct DataLake {
    pub base_dir: PathBuf,

}


impl DataLake {
    /// Create a new DataLake rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Given a logical dataset id, derive the CSV file path.
    ///
    /// Example convention:
    ///   id = "nyc_taxi_2024_01"  →  "nyc_taxi_2024_01.csv"
    pub fn dataset_path(&self, dataset_id: &str) -> PathBuf {
        let file_name = format!("{dataset_id}.csv");
        self.base_dir.join(file_name)
    }

    /// Load the dataset with the given id as a Polars DataFrame.
    ///
    /// This is stateless: every call is independent and just reads from disk.
    pub fn load_dataset(&self, dataset_id: &str) -> PolarsResult<DataFrame> {
        let path: PathBuf = self.dataset_path(dataset_id);

        // Eager CSV read with options (has_header = true)
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(path))?
            .finish()
    }

    /// Optional helper: check if a dataset exists on disk.
    pub fn dataset_exists(&self, dataset_id: &str) -> bool {
        self.dataset_path(dataset_id).is_file()
    }
}

#[derive(Clone)]
pub struct AppState {
    pub lake: Arc<DataLake>,
}

impl AppState {
    /// Create `AppState` with a given base directory for CSVs.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        let lake = DataLake::new(base_dir);
        Self {
            lake: Arc::new(lake),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RegressionRequest {
    x_col: String,
    y_col: String,
    // You could add filters or ranges later if you want
}

#[derive(Debug, Serialize)]
struct RegressionResult {
    slope: f64,
    intercept: f64,
    r2: Option<f64>,
}

#[derive(Deserialize, Eq, PartialEq)]
enum SortingOptions {
    Desc,
    Asc,
}

#[derive(Deserialize)]
struct DelayOptions {
    sorting: Option<SortingOptions>,
    limit: Option<u32>,
}
#[derive(Debug, Deserialize)]
struct DataPreviewOptions {
    limit: Option<u32>,
}


async fn get_data_preview(
    State(state): State<AppState>,
    Path(data_id): Path<String>,
    Query(opts): Query<DataPreviewOptions>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut df = state
        .lake
        .load_dataset(&data_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let limit = opts.limit.unwrap_or(100);
    df = df.head(Some(limit as usize));

    let rows = dataframe_to_json_rows(&df);
    Ok(Json(rows))
}


async fn run_regression(
    State(state): State<AppState>,
    Path(data_id): Path<String>,
    Json(req): Json<RegressionRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let df = state
        .lake
        .load_dataset(&data_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let x_series = df
        .column(&req.x_col)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .cast(&DataType::Float64)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let y_series = df
        .column(&req.y_col)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .cast(&DataType::Float64)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let x = x_series.f64().map_err(|_| StatusCode::BAD_REQUEST)?;
    let y = y_series.f64().map_err(|_| StatusCode::BAD_REQUEST)?;

    let n = x.len();

    if n == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Simple OLS: compute sums
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let mut sum_y2 = 0.0;

    for i in 0..n {
        let xi = x.get(i).unwrap_or(0.0);
        let yi = y.get(i).unwrap_or(0.0);

        sum_x += xi;
        sum_y += yi;
        sum_xy += xi * yi;
        sum_x2 += xi * xi;
        sum_y2 += yi * yi;
    }

    let n_f = n as f64;
    let denom = n_f * sum_x2 - sum_x * sum_x;

    if denom == 0.0 {
        return Err(StatusCode::BAD_REQUEST); // X has no variance
    }

    let slope = (n_f * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n_f;

    // Optional: compute R^2
    let ss_tot = n_f * sum_y2 - sum_y * sum_y;
    let ss_res = (0..n).fold(0.0, |acc, i| {
        let xi = x.get(i).unwrap_or(0.0);
        let yi = y.get(i).unwrap_or(0.0);
        let y_pred = slope * xi + intercept;
        acc + (yi - y_pred).powi(2)
    });

    let r2 = if ss_tot != 0.0 {
        Some(1.0 - ss_res / ss_tot)
    } else {
        None
    };

    Ok(Json(RegressionResult {
        slope,
        intercept,
        r2,
    }))
}



fn dataframe_to_json_rows(df: &DataFrame) -> Vec<HashMap<String, JsonValue>> {
    let height = df.height();
    let columns = df.get_columns();

    let mut rows = Vec::with_capacity(height);

    for row_idx in 0..height {
        let mut row_map = HashMap::new();

        for col in columns {
            let name = col.name().to_string();

            // In polars 0.42, Series::get returns Result<AnyValue<'_>, PolarsError>
            // On error, treat the value as Null.
            let val = col.get(row_idx).unwrap_or(AnyValue::Null);

            let json_val = match val {
                AnyValue::Null => JsonValue::Null,
                AnyValue::Boolean(b) => JsonValue::Bool(b),

                AnyValue::Int64(i) => JsonValue::from(i),
                AnyValue::Int32(i) => JsonValue::from(i),
                AnyValue::Int16(i) => JsonValue::from(i),
                AnyValue::Int8(i) => JsonValue::from(i),

                AnyValue::UInt64(i) => JsonValue::from(i),
                AnyValue::UInt32(i) => JsonValue::from(i),
                AnyValue::UInt16(i) => JsonValue::from(i),
                AnyValue::UInt8(i) => JsonValue::from(i),

                AnyValue::Float64(f) => JsonValue::from(f),
                AnyValue::Float32(f) => JsonValue::from(f as f64),

                // String variants in polars 0.42:
                AnyValue::String(s) => JsonValue::from(s),
                AnyValue::StringOwned(s) => JsonValue::from(s.as_str()),

                // Fallback for everything else (dates, lists, structs, etc.)
                other => JsonValue::from(other.to_string()),
            };

            row_map.insert(name, json_val);
        }

        rows.push(row_map);
    }

    rows
}




async fn get_sorted_delays(
    State(state): State<AppState>,
    Path(data_id): Path<String>,
    Query(opts): Query<DelayOptions>,
) -> Result<impl IntoResponse, StatusCode> {
    // Load the dataset (blocking; in a real app consider spawn_blocking).
    let df = state
        .lake
        .load_dataset(&data_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Ensure there is a "delay" column
    if !df.get_column_names().iter().any(|name| name.contains("delay")) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Determine sort direction
    let descending = matches!(opts.sorting, Some(SortingOptions::Desc));
let delay_cols: Vec<String> = df
    .get_column_names()
    .iter()
    .filter(|name| name.contains("delay"))
    .map(|name| name.to_string())
    .collect();
    // Sort by "delay"
let mut lf = if delay_cols.is_empty() {
    df.lazy()
} else {
    df.lazy().sort(
        delay_cols,
        SortMultipleOptions::new().with_order_descending(descending),
    )
};

    // Apply limit if present
    if let Some(limit) = opts.limit {
        lf = lf.slice(0, limit as u32);
    }

    let sorted_df = lf
        .collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = dataframe_to_json_rows(&sorted_df);

    Ok(Json(rows))
}


fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(Html(INDEX_HTML)))
        // Preview the raw data for a given dataset
        .route("/data/:data_id/preview", get(get_data_preview))
        // Sorted delays endpoint
        .route("/data/:data_id/delays", get(get_sorted_delays))
        // Linear regression endpoint
        .route("/data/:data_id/regression", post(run_regression))
        .with_state(state)
}

#[tokio::main]
async fn main() {
    let state = AppState::new("./data");

    let app = build_router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await.unwrap();

    println!("Listening on http://{}", addr);

    axum::serve(listener, app)
        .await
        .unwrap();
}

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <title>Rust + Polars Data Explorer</title>
    <style>
        body {
            font-family: system-ui, sans-serif;
            max-width: 960px;
            margin: 2rem auto;
            padding: 0 1rem;
        }
        h1 { margin-bottom: 0.5rem; }
        section {
            border: 1px solid #ccc;
            padding: 1rem;
            margin-bottom: 1.5rem;
            border-radius: 8px;
        }
        label { display: block; margin: 0.25rem 0; }
        input, select, button {
            margin-top: 0.25rem;
        }
        pre {
            background: #f5f5f5;
            padding: 0.75rem;
            border-radius: 6px;
            white-space: pre-wrap;
            max-height: 300px;
            overflow: auto;
        }
    </style>
</head>
<body>
    <h1>Rust + Polars Data Explorer</h1>
    <p>
        This frontend talks to a Rust + Axum + Polars backend.
        Provide a <code>data_id</code> corresponding to a CSV like <code>./data/&lt;data_id&gt;.csv</code>.
        For example, if you have <code>./data/trains.csv</code>, use <code>trains</code>.
    </p>

    <section>
        <h2>1. Preview Dataset</h2>
        <label>
            Dataset ID:
            <input id="preview_data_id" type="text" placeholder="e.g. trains" />
        </label>
        <label>
            Limit rows:
            <input id="preview_limit" type="number" value="20" />
        </label>
        <button onclick="previewData()">Preview</button>
        <pre id="preview_output"></pre>
    </section>

    <section>
        <h2>2. Sort by Delay</h2>
        <label>
            Dataset ID:
            <input id="delay_data_id" type="text" placeholder="e.g. trains" />
        </label>
        <label>
            Sorting:
            <select id="delay_sorting">
                <option value="">(none)</option>
                <option value="Asc">Ascending</option>
                <option value="Desc">Descending</option>
            </select>
        </label>
        <label>
            Limit rows:
            <input id="delay_limit" type="number" value="20" />
        </label>
        <button onclick="loadDelays()">Load Sorted Delays</button>
        <pre id="delay_output"></pre>
    </section>

    <section>
        <h2>3. Linear Regression</h2>
        <label>
            Dataset ID:
            <input id="reg_data_id" type="text" placeholder="e.g. trains" />
        </label>
        <label>
            X column:
            <input id="reg_x_col" type="text" placeholder="e.g. scheduled_departure_time" />
        </label>
        <label>
            Y column:
            <input id="reg_y_col" type="text" placeholder="e.g. delay" />
        </label>
        <button onclick="runRegression()">Run Regression</button>
        <pre id="reg_output"></pre>
    </section>

    <script>
        async function previewData() {
            const dataId = document.getElementById('preview_data_id').value.trim();
            const limit = document.getElementById('preview_limit').value.trim();
            const out = document.getElementById('preview_output');
            out.textContent = 'Loading...';

            if (!dataId) {
                out.textContent = 'Please enter a dataset ID.';
                return;
            }

            const params = new URLSearchParams();
            if (limit) params.set('limit', limit);

            try {
                const res = await fetch(`/data/${encodeURIComponent(dataId)}/preview?` + params.toString());
                if (!res.ok) {
                    out.textContent = 'Error: ' + res.status + ' ' + res.statusText;
                    return;
                }
                const json = await res.json();
                out.textContent = JSON.stringify(json, null, 2);
            } catch (err) {
                out.textContent = 'Request failed: ' + err;
            }
        }

        async function loadDelays() {
            const dataId = document.getElementById('delay_data_id').value.trim();
            const sorting = document.getElementById('delay_sorting').value;
            const limit = document.getElementById('delay_limit').value.trim();
            const out = document.getElementById('delay_output');
            out.textContent = 'Loading...';

            if (!dataId) {
                out.textContent = 'Please enter a dataset ID.';
                return;
            }

            const params = new URLSearchParams();
            if (sorting) params.set('sorting', sorting);
            if (limit) params.set('limit', limit);

            try {
                const res = await fetch(`/data/${encodeURIComponent(dataId)}/delays?` + params.toString());
                if (!res.ok) {
                    out.textContent = 'Error: ' + res.status + ' ' + res.statusText;
                    return;
                }
                const json = await res.json();
                out.textContent = JSON.stringify(json, null, 2);
            } catch (err) {
                out.textContent = 'Request failed: ' + err;
            }
        }

        async function runRegression() {
            const dataId = document.getElementById('reg_data_id').value.trim();
            const xCol = document.getElementById('reg_x_col').value.trim();
            const yCol = document.getElementById('reg_y_col').value.trim();
            const out = document.getElementById('reg_output');
            out.textContent = 'Loading...';

            if (!dataId || !xCol || !yCol) {
                out.textContent = 'Please enter dataset ID, x_col, and y_col.';
                return;
            }

            try {
                const res = await fetch(`/data/${encodeURIComponent(dataId)}/regression`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ x_col: xCol, y_col: yCol })
                });
                if (!res.ok) {
                    out.textContent = 'Error: ' + res.status + ' ' + res.statusText;
                    return;
                }
                const json = await res.json();
                out.textContent = JSON.stringify(json, null, 2);
            } catch (err) {
                out.textContent = 'Request failed: ' + err;
            }
        }
    </script>
</body>
</html>
"#;
