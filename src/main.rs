use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use std::fs::File;
use std::io::Read;
use async_ftp::FtpStream;
use rusoto_s3::{S3Client, S3, PutObjectRequest, GetObjectRequest, ListObjectsV2Request};
use rusoto_core::{Region, ByteStream};
use tokio::io::AsyncReadExt;


#[derive(serde::Deserialize)]
struct Config {
    ftp_server: String,
    ftp_username: String,
    ftp_password: String,
    ftp_path: String,
    s3_bucket: String,
    s3_prefix: String,
}

struct LocationPair {
    s3_location: String,
    ftp_location: String,
    // Additional configuration if needed
}

fn main() {
    let config = load_config(); // Implement this based on your configuration source
    let rt = Runtime::new().unwrap(); // Create a Tokio runtime for async operations

    loop {
        let config = Arc::new(config); // Use Arc to safely share config across threads

        let handles: Vec<_> = config.pairs.iter().map(|pair| {
            let pair = Arc::new(pair.clone());
            let config_clone = Arc::clone(&config);
            let rt_clone = rt.clone();

            thread::spawn(move || {
                rt_clone.block_on(sync_locations(pair, config_clone));
            })
        }).collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Sleep until the next interval
        thread::sleep(Duration::from_minutes(config.interval_minutes));
    }
}

async fn sync_locations(pair: Arc<LocationPair>, _config: Arc<Config>) {
    let config = load_config();
    let s3_client = S3Client::new(Region::UsEast1); // Adjust the region accordingly

    // Example config values
    let ftp_server = config.ftp_server;
    let ftp_username = config.ftp_username;
    let ftp_password = config.ftp_password;
    let ftp_path = config.ftp_path;
    let s3_bucket = config.s3_bucket;
    let s3_prefix = config.s3_prefix;

    let s3_files = list_files_s3(s3_bucket, s3_key_prefix).await;
    let ftp_files = list_files_ftp(ftp_server, ftp_username, ftp_password, ftp_path).await;

    // Compare lists and identify files to sync
    let s3_only = s3_files.iter().filter(|f| !ftp_files.contains(f)).collect::<Vec<_>>();
    let ftp_only = ftp_files.iter().filter(|f| !s3_files.contains(f)).collect::<Vec<_>>();

    // Sync files from S3 to FTP
    for file in &s3_only {
        let data = download_from_s3(&s3_client, &s3_bucket, &(s3_prefix.clone() + file)).await;
        upload_to_ftp(&ftp_server, &ftp_username, &ftp_password, &ftp_path, file, data).await;
        println!("Synced {} from S3 to FTP", file);
    }

    // Sync files from FTP to S3
    for file in ftp_only {
        let data = download_from_ftp(&ftp_server, &ftp_username, &ftp_password, &ftp_path, &file).await;
        upload_to_s3(&s3_client, &s3_bucket, &(s3_prefix.clone() + &file), data).await;
        println!("Synced {} from FTP to S3", file);
    }

    // Note: Implement the actual file transfer logic based on your FTP and S3 library's capabilities
}

fn load_config() -> Config {
    let mut file = File::open("config.json").expect("Unable to open config file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read config file");
    serde_json::from_str(&contents).expect("Error parsing config file")
}

// Implement any additional functions needed for listing, comparing, and moving files
async fn list_files_s3(bucket: &str, prefix: &str) -> Vec<String> {
    let client = S3Client::new(Region::UsEast1); // Adjust the region accordingly
    let list_req = ListObjectsV2Request {
        bucket: bucket.to_string(),
        prefix: Some(prefix.to_string()),
        ..Default::default()
    };

    match client.list_objects_v2(list_req).await {
        Ok(output) => {
            output.contents.unwrap_or_else(|| vec![])
                .into_iter()
                .filter_map(|object| object.key)
                .collect()
        },
        Err(e) => {
            eprintln!("Error listing S3 objects: {}", e);
            vec![]
        }
    }
}

async fn list_files_ftp(server: &str, username: &str, password: &str, path: &str) -> Vec<String> {
    let mut ftp_stream = FtpStream::connect(server).await.unwrap();
    ftp_stream.login(username, password).await.unwrap();
    let files = ftp_stream.list(Some(path)).await.unwrap();
    ftp_stream.logout().await.unwrap();

    files.into_iter().map(|file| file.name).collect()
}

async fn download_from_s3(client: &S3Client, bucket: &str, key: &str) -> bytes::Bytes {
    let get_req = GetObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..Default::default()
    };

    let result = client.get_object(get_req).await.unwrap();
    let stream = result.body.unwrap();
    let bytes = stream.map_ok(|b| bytes::BytesMut::from(&b[..])).try_concat().await.unwrap();
    bytes.freeze()
}

async fn upload_to_ftp(ftp_server: &str, ftp_username: &str, ftp_password: &str, ftp_path: &str, file_name: &str, data: bytes::Bytes) {
    let mut ftp_stream = FtpStream::connect(ftp_server).await.unwrap();
    ftp_stream.login(ftp_username, ftp_password).await.unwrap();
    let full_path = format!("{}/{}", ftp_path, file_name);
    ftp_stream.put(full_path, data.reader()).await.unwrap();
    ftp_stream.logout().await.unwrap();
}

async fn upload_to_s3(client: &S3Client, bucket: &str, key: &str, data: bytes::Bytes) {
    let put_req = PutObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(ByteStream::from(data)),
        ..Default::default()
    };

    client.put_object(put_req).await.unwrap();
}

async fn download_from_ftp(ftp_server: &str, ftp_username: &str, ftp_password: &str, ftp_path: &str, file_name: &str) -> bytes::Bytes {
    let mut ftp_stream = FtpStream::connect(ftp_server).await.unwrap();
    ftp_stream.login(ftp_username, ftp_password).await.unwrap();
    let full_path = format!("{}/{}", ftp_path, file_name);
    let data = ftp_stream.simple_retr(&full_path).await.unwrap();
    ftp_stream.logout().await.unwrap();
    data.into_bytes()
}