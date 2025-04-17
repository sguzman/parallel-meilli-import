fn main() {
    println!("Hello, world!");
    let dotenv = dotenv::dotenv();
    dotenv.ok().expect("Failed to load .env file");

    let url = dotenv::var("URL").expect("URL not set");
    let port = dotenv::var("PORT").expect("PORT not set");
    let api = dotenv::var("API").expect("API not set");

    let url = build_url(&url, &port);
    let client = build_connection(&url, &api);

    println!("Connecting to {} on port {}", url, port);

    println!("Goodbye, world!");
}

fn build_url(url: &str, port: &str) -> String {
    format!("http://{}:{}", url, port)
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
