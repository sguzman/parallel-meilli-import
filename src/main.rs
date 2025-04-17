use std::path::PathBuf;

use clap::Parser;

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

struct Input {
    path: PathBuf,
    address: String,
    api: String,
    index: String,
}

struct DB {
    address: String,
    api: String,
}

struct Task {
    db: DB,
    index: String,
}

fn build_db(input: &Input) -> DB {
    DB {
        address: input.address.clone(),
        api: input.api.clone(),
    }
}

// Generate a random 5 letter string
fn generate_random_string() -> String {
    let chars = "abcdefghijklmnopqrstuvwxyz";
    let random_string: String = (0..5)
        .map(|_| {
            let idx = rand::random::<usize>() % chars.len();
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

fn main() {
    println!("Hello, world!");

    let Input {
        path,
        address,
        api,
        index,
    } = init();

    let json_data = load_data(&path);

    let db = DB { address, api };
    let client = create_db(&db);

    // Print info about the client
    println!("Client: {:?}", client);

    println!("Goodbye, world!");
}

fn build_url(url: &str, port: &str) -> String {
    format!("http://{}:{}", url, port)
}

fn create_db(db: &DB) -> meilisearch_sdk::client::Client {
    build_connection(&db.address, &db.api)
}

fn build_connection(url: &str, api: &str) -> meilisearch_sdk::client::Client {
    let key = Some(api);
    let opt = meilisearch_sdk::client::Client::new(url, key);

    if let Err(e) = opt {
        println!("Failed to connect to Meilisearch: {}", e);
        std::process::exit(1);
    }
    opt.unwrap()
}
