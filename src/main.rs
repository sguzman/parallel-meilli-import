use std::path::PathBuf;

use clap::Parser;
use meilisearch_sdk::{errors::Error, task_info::TaskInfo};
use rand::Rng;

#[derive(Parser)]
#[command(name = "Parallel Meilisearch Import")]
#[command(bin_name = "parallel-meil")]
#[command(color = clap::ColorChoice::Always)]
#[command(about = "Import data into Meilisearch in parallel")]
#[command(author = "Salvador Guzman")]
#[command(version = "1.0")]
#[command(long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long)]
    input: PathBuf,

    // Name of meilisearch index
    #[arg(short, long)]
    index: Option<String>,

    // Number of threads
    #[arg(short, long, default_value_t = 8)]
    threads: usize,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArxivEntry {
    // Since abstract is a reserved word in Rust, we use `abstract_text` instead
    #[serde(rename = "abstract")]
    pub abstract_text: String,
    pub authors: String,
    pub authors_parsed: Vec<Vec<String>>,
    pub categories: String,
    pub comments: Option<String>,
    pub doi: Option<String>,
    pub id: u32,
    pub journal_ref: Option<String>,
    pub license: Option<String>,
    pub report_no: Option<String>,
    pub submitter: String,
    pub title: String,
    pub update_date: String,
    pub versions: Vec<Version>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version {
    pub created: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Input {
    path: PathBuf,
    address: String,
    api: String,
    index: String,
    threads: usize,
}

// Generate a random 5 letter string
fn generate_random_string() -> String {
    let chars = "abcdefghijklmnopqrstuvwxyz";
    let random_string: String = (0..5)
        .map(|_| {
            let idx = rand::rng().random_range(0..chars.len());
            chars.chars().nth(idx).unwrap()
        })
        .collect();
    random_string
}

fn init() -> Input {
    let matches = Cli::parse();
    let path = matches.input;
    let index = matches.index.unwrap_or_else(|| generate_random_string());
    let threads = matches.threads;

    let dotenv = dotenv::dotenv();
    dotenv.ok().expect("Failed to load .env file");

    let url = dotenv::var("URL").expect("URL not set");
    let port = dotenv::var("PORT").expect("PORT not set");
    let api = dotenv::var("API").expect("API not set");

    let url = build_url(&url, &port);
    println!("Connecting to {}", url);

    Input {
        path,
        address: url,
        api,
        index,
        threads,
    }
}

// Load JSON data from a file
fn load_data(path: &PathBuf) -> Vec<ArxivEntry> {
    let data = std::fs::read_to_string(path).expect("Failed to read file");
    let data: Vec<ArxivEntry> = serde_json::from_str(&data).expect("Failed to parse JSON");
    data
}

// Task of insertion into Meilisearch a single item
// Should initialize a new client
// and insert the item into the index
async fn insert_item(input: &Input, name: &str, item: &ArxivEntry) -> Result<TaskInfo, Error> {
    let db = build_connection(input);

    // Pass the struct directly instead of serializing it
    let table = db.index(name);
    table.add_documents(&[item], Some("id")).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Hello, world!");

    let input = init();
    let path = input.path.clone();
    let index = input.index.clone();

    // Print the input data
    println!("Input: {:#?}", input);

    let json_data = load_data(&path);

    // Create a vector to hold all the task handles
    let mut tasks = Vec::new();

    for item in json_data {
        let input_clone = input.clone();
        let index_clone = index.clone();
        let item_clone = item.clone();

        // Spawn a new task for each item
        let task = tokio::spawn(async move {
            match insert_item(&input_clone, &index_clone, &item_clone).await {
                Ok(_) => println!("Inserted item with id: {}", item_clone.id),
                Err(e) => eprintln!("Failed to insert item with id {}: {}", item_clone.id, e),
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        if let Err(e) = task.await {
            eprintln!("Task failed: {}", e);
        }
    }

    println!("Goodbye, world!");
}

fn build_url(url: &str, port: &str) -> String {
    format!("http://{}:{}", url, port)
}

fn build_connection(input: &Input) -> meilisearch_sdk::client::Client {
    let api = input.api.clone();
    let url = input.address.clone();
    let key = Some(api);

    let opt = meilisearch_sdk::client::Client::new(url, key);

    if let Err(e) = opt {
        println!("Failed to connect to Meilisearch: {}", e);
        std::process::exit(1);
    }
    opt.unwrap()
}
