use std::path::PathBuf;

use clap::Parser;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Input {
    path: PathBuf,
    address: String,
    api: String,
    index: String,
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
    }
}

// Load JSON data from a file
fn load_data(path: &PathBuf) -> Vec<serde_json::Value> {
    let data = std::fs::read_to_string(path).expect("Failed to read file");
    let data: Vec<serde_json::Value> = serde_json::from_str(&data).expect("Failed to parse JSON");
    data
}

// Task of insertion into Meilisearch a single item
// Should initialize a new client
// and insert the item into the index
#[tokio::main(flavor = "current_thread")]
async fn insert_item(input: &Input, name: &str, item: &serde_json::Value) {
    let db = build_connection(&input);
    let task = serde_json::to_string(&item).unwrap();

    let table = db.index(name);
    let task = table.add_documents(&[task], Some("id")).await.unwrap();
    println!("Task: {:#?}", task);
}

fn main() {
    println!("Hello, world!");

    let input = init();
    let path = input.path.clone();
    let address = input.address.clone();
    let api = input.api.clone();
    let index = input.index.clone();

    // Print the input data
    println!("Path: {:?}", path);
    println!("Address: {}", address);
    println!("API: {}", api);
    println!("Index: {}", index);

    let json_data = load_data(&path);

    for item in json_data {
        insert_item(&input, &index, &item);
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
