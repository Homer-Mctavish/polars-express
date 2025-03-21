use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::fs::File;
use std::io::Write;
use tokio::runtime::Runtime;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    // Example usage:
    let start_url = "https://example.com";
    let save_directory = "csv_downloads";
    let context_keyword = Some("reports"); // or None if you don't need a keyword

    // Create an async reqwest client
    let client = Client::new();

    // Crawl the page for CSV links
    let csv_links = crawl_for_csv_links(&client, start_url, context_keyword).await?;
    println!("Found {} CSV links.", csv_links.len());

    // Download each CSV
    for link in csv_links {
        download_csv(&client, &link, save_directory).await?;
    }

    Ok(())
}

/// Crawls a single URL, returns all CSV links that match optional context filter.
async fn crawl_for_csv_links(
    client: &Client,
    url: &str,
    context_keyword: Option<&str>,
) -> Result<Vec<String>> {
    // 1. Fetch the HTML
    let resp = client.get(url).send().await?;
    let html_body = resp.text().await?;

    // 2. Parse the HTML with `scraper`
    let document = Html::parse_document(&html_body);

    // 3. Select all <a> tags
    let selector_a = Selector::parse("a").unwrap();
    let mut csv_links = Vec::new();

    for element in document.select(&selector_a) {
        if let Some(href) = element.value().attr("href") {
            // Check if the link text or href contains the context keyword
            let link_text = element.text().collect::<Vec<_>>().join(" ");
            let keyword_match = if let Some(keyword) = context_keyword {
                // Check if the text or href has the keyword (case-insensitive)
                href.to_lowercase().contains(&keyword.to_lowercase())
                    || link_text.to_lowercase().contains(&keyword.to_lowercase())
            } else {
                // No keyword filter
                true
            };

            // Does the link end with .csv or is it a possible CSV (with query params, etc.)?
            let ends_with_csv = href.to_lowercase().ends_with(".csv");

            // If there's a keyword, we might also consider a link with "csv" in it, but let's keep it simple:
            if ends_with_csv || keyword_match {
                // Convert relative link to absolute
                if let Ok(abs_link) = make_absolute_url(url, href) {
                    // If `ends_with_csv` is false but there's a match on the keyword,
                    // you could do extra checks here to see if the link is truly CSV, etc.
                    csv_links.push(abs_link);
                }
            }
        }
    }

    // Optionally deduplicate
    csv_links.sort();
    csv_links.dedup();

    Ok(csv_links)
}

/// Joins a possibly relative `href` to the base page URL.
fn make_absolute_url(base: &str, href: &str) -> Result<String> {
    let base_url = Url::parse(base)?;
    let joined = base_url.join(href)?;
    Ok(joined.to_string())
}

/// Downloads the CSV file from `url` and saves to `save_dir`.
async fn download_csv(client: &Client, url: &str, save_dir: &str) -> Result<()> {
    // 1. Fetch
    println!("Downloading: {}", url);
    let resp = client.get(url).send().await?;
    let bytes = resp.bytes().await?;

    // 2. Create the local file name
    let filename = url
        .split('/')
        .last()
        .unwrap_or("unknown.csv"); // Fallback name if split fails
    let path = std::path::Path::new(save_dir).join(filename);

    // Create the directory structure if not existing
    std::fs::create_dir_all(save_dir)?;

    // 3. Write to file
    let mut file = File::create(&path)?;
    file.write_all(&bytes)?;

    println!("Saved to {:?}", path);
    Ok(())
}
