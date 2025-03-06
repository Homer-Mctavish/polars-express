// scraper.rs

use reqwest::Client;
use scraper::{Html, Selector};
use std::{sync::Arc, time::Instant};
use axum::{Router, routing::get, extract::Path, Json, State};
use serde_json::Value;
use tracing::info;
use futures::TryStreamExt;

async fn scrape_links(url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let body = client.get(url).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let selector = Selector::parse("a").unwrap();

    let links: Vec<String> = document
        .select(&selector)
        .filter_map(|element| {
            element
                .value()
                .attr("href")
                .map(|link| link.to_string())
        })
        .collect();

    Ok(links)
}