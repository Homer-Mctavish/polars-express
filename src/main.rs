use axum::{routing::get, Router};
use polars::prelude::*;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::info;
use tracing::error;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;
use reqwest::Client;
use scraper::{Html, Selector};
use std::sync::Mutex;
use reqwest::Error;
// use std::fs::File;
// use tokio::fs::File;

#[derive(Clone)]
struct AppState {
    df_delays: DataFrame,
    df_cities: DataFrame,
    shared_data: Arc<Mutex<Vec<String>>>
}

impl AppState {
    fn new(df_delays: DataFrame, df_cities: DataFrame, shared_data: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            df_delays,
            df_cities,
            shared_data
        }
    }
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let now = Instant::now();
    info!("reading delays csv dataframe");

    let df_delays = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("DBtrainrides.csv".into()))
    .expect("can read delays csv")
    .finish()
    .expect("can create delays dataframe");

    let df_cities = CsvReadOptions::default()
    .with_has_header(true)
    .try_into_reader_with_file_path(Some("plz_einwohner.csv".into()))
    .expect("can read cities csv")
    .finish()
    .expect("can create cities dataframe");

    info!("done reading csv {:?}", now.elapsed());


    // Scrape links from a website asynchronously
    let scrape_url = "http://example.com"; // Use your desired URL
    let links = match scrape_links(scrape_url).await {
        Ok(links) => {
            info!("Scraped links: {:?}", links);
            links
        }
        Err(err) => {
            info!("Error scraping links: {}", err);
            Vec::new()
        }
    };
    

    let app_state = Arc::new(AppState::new(df_delays, df_cities, Arc::new(Mutex::new(links))));

    let app = Router::new()
      .route("/delays", get(delays))
      .route("/cities/:zip/population", get(cities_pop))
      .route("/cities/:zip/area", get(cities_area))
      .route("/delays/:zip/", get(delays_by_zip))
      .route("/delays/corr/:field1/:field2/", get(delays_corr))
      .route("/links/:url", get(scrape_links))
      .route("/batches", get(batch_download_csvs))
      .with_state(app_state);


    let listener = TcpListener::bind("0.0.0.0:8000")
    .await
    .expect("can start web server on port 8000");

info!("listening on {:?}", listener.local_addr());

axum::serve(listener, app)
    .await
    .expect("can start web server");
}

async fn delays(
    query: Query<DelayOptions>,
    app_state: State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    let mut sorting_mode = SortMultipleOptions::default()
    .with_order_descending(true)
    .with_nulls_last(true);
if let Some(query_sorting) = &query.sorting {
    if *query_sorting == SortingOptions::Asc {
        sorting_mode = SortMultipleOptions::default()
            .with_order_descending(false)
            .with_nulls_last(true);
    }
}
let mut limit = 10;
if let Some(query_limit) = query.limit {
    limit = query_limit
}
let delays = app_state
.df_delays
.clone()
.lazy()
.sort(["arrival_delay_m"], sorting_mode)
.select([cols([
    "ID",
    "arrival_delay_m",
    "departure_delay_m",
    "city",
    "zip",
])])
.limit(limit)
.collect()
.map_err(|e| {
    error!("could not query delay times: {}", e);
    StatusCode::BAD_REQUEST
})?;

df_to_json(delays)
}

// Helper function to convert DataFrame to JSON
fn df_to_json(df: DataFrame) -> Result<Json<Value>, StatusCode> {
    // Try to serialize the DataFrame to a JSON value
    match serde_json::to_value(&df) {
        Ok(value) => Ok(Json(value)), // Wrap the value in a Json response
        Err(e) => {
            error!("could not serialize dataframe to json: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn cities_zip_to(df_cities: &DataFrame, zip: &str, field: &str) -> Result<DataFrame, StatusCode> {
    df_cities
        .clone()
        .lazy()
        .filter(col("zip").str().contains_literal(lit(zip)))
        .select([col("zip"), col(field)])
        .collect()
        .map_err(|e| {
            error!("could not query {} for zip: {}", field, e);
            StatusCode::BAD_REQUEST
        })
}

async fn cities_pop(
    Path(zip): Path<String>,
    app_state: State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    let pop_for_zip = cities_zip_to(&app_state.as_ref().df_cities, &zip, "pop")?;
    df_to_json(pop_for_zip)
}

async fn cities_area(
    Path(zip): Path<String>,
    app_state: State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    let area_for_zip = cities_zip_to(&app_state.as_ref().df_cities, &zip, "area")?;
    df_to_json(area_for_zip)
}

async fn delays_by_zip(
    Path(zip): Path<String>,
    app_state: State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    let delays_for_zip = app_state
        .as_ref()
        .df_delays
        .clone()
        .lazy()
        .join(
            app_state.as_ref().df_cities.clone().lazy(),
            [col("zip")],
            [col("zip")],
            JoinArgs::new(JoinType::Inner),
        )
        .group_by(["zip"])
        .agg([
            len().alias("len"),
            col("arrival_delay_m")
                .cast(DataType::Int64)
                .sum()
                .alias("sum_arrival_delays"),
            col("arrival_delay_m").alias("max").max(),
            col("pop").unique().first(),
            col("city").unique().first(),
        ])
        .filter(col("zip").str().contains_literal(lit(zip.as_str())))
        .collect()
        .map_err(|e| {
            error!("could not query delay times by zip {}: {}", zip, e);
            StatusCode::BAD_REQUEST
        })?;

    df_to_json(delays_for_zip)
}

async fn delays_corr(
    Path((field_1, field_2)): Path<(String, String)>,
    app_state: State<Arc<AppState>>,
) -> Result<Json<Value>, StatusCode> {
    let corr = app_state
        .as_ref()
        .df_delays
        .clone()
        .lazy()
        .join(
            app_state.as_ref().df_cities.clone().lazy(),
            [col("zip")],
            [col("zip")],
            JoinArgs::new(JoinType::Inner),
        )
        .select([
            len(),
            pearson_corr(col(&field_1), col(&field_2), 0)
                .alias(&format!("corr_{}_{}", field_1, field_2)),
        ])
        .collect()
        .map_err(|e| {
            error!(
                "could not query delay time correlation between {} and {}: {}",
                field_1, field_2, e
            );
            StatusCode::BAD_REQUEST
        })?;

    df_to_json(corr)
}


async fn scrape_links(url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = client.get(url).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let selecto = Selector::parse("a").unwrap();

    let links: Vec<String> = document
        .select(&selecto)
        .filter_map(|element| {
            element
                .value()
                .attr("href")
                .map(|link| link.to_string())
        })
        .collect();

    Ok(links)
}

/// Attempts to download each link in `links` as a CSV.
/// Returns a vector of Results (one per link) so you can see which succeeded or failed.
pub async fn batch_download_csvs(links: &[String], save_dir: &str) -> Vec<Result<(), Error>> {
    let client = Client::new();
    let mut results = Vec::with_capacity(links.len());

    // Ensure the directory exists
    std::fs::create_dir_all(save_dir).ok();

    for link in links {
        let r = download_single_csv(&client, link, save_dir).await;
        results.push(r);
    }

    results
}

/// Helper: downloads a single CSV link and saves it in `save_dir`.
async fn download_single_csv(client: &Client, link: &str, save_dir: &str) -> Result<(), Error> {
    // Attempt to GET
    println!("Downloading: {}", link);
    let resp = client.get(link).send().await?;
    
    // Check content type? (Optional)
    // if let Some(ct) = resp.headers().get(reqwest::header::CONTENT_TYPE) {
    //     if !ct.to_str()?.contains("text/csv") {
    //         anyhow::bail!("Not a CSV content type: {:?}", ct);
    //     }
    // }

    let bytes = resp.bytes().await?;

    // Derive local filename
    let filename = link.split('/').last().unwrap_or("unknown.csv");
    let path = std::path::Path::new(save_dir).join(filename);

    // Write to file
    let mut file = File::create(&path)?;
    file.write_all(&bytes)?;

    println!("Saved {}", path.display());
    Ok(())
}