use anyhow::Result;
use common::ws_client::{self, RecvMessage, SentMessage};
use csv::Writer;
use futures::StreamExt;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::sleep;

struct Config {
    genesis_hash: String,
    telemetry_url: String,
    output_path: PathBuf,
    nodes_file: PathBuf,
    blocks_file: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            genesis_hash: "0xdbacc01ae41b79388135ccd5d0ebe81eb0905260344256e6f4003bb8e75a91b5"
                .to_string(),
            telemetry_url: "wss://tc0.res.fm/feed".to_string(),
            output_path: PathBuf::from("./data/res-likely-authors.csv"),
            nodes_file: PathBuf::from("./data/telemetry-nodes.json"),
            blocks_file: PathBuf::from("./data/telemetry-blocks.json"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeInfo {
    name: String,
    node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockReporter {
    node_idx: u64,
    node_name: String,
    node_id: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockInfo {
    block_number: u64,
    lowest_prop_time: u64,
    reporters: Vec<BlockReporter>,
    first_seen: u64,
    report_count: u64,
    output: bool,
}

#[derive(Debug)]
struct TelemetryObserver {
    genesis_hash: String,
    nodes_file: PathBuf,
    blocks_file: PathBuf,
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    blocks: Arc<Mutex<HashMap<String, BlockInfo>>>,
    csv_writer: Arc<Mutex<Writer<File>>>,
}

impl TelemetryObserver {
    async fn new(config: Config) -> Result<Self> {
        debug!("TelemetryObserver::new() called");
        // Load or initialize nodes
        let nodes = if config.nodes_file.exists() {
            let file = File::open(&config.nodes_file)?;
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Load or initialize blocks
        let blocks = if config.blocks_file.exists() {
            let file = File::open(&config.blocks_file)?;
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Initialize CSV writer
        info!("Initializing CSV writer at {:?}", config.output_path);
        let csv_exists = config.output_path.exists() && config.output_path.metadata()?.len() > 0;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.output_path)?;
        let mut csv_writer = Writer::from_writer(file);

        // Write header if file is new
        if !csv_exists {
            csv_writer.write_record(&[
                "timestamp",
                "node_name",
                "node_id",
                "block_number",
                "block_hash",
                "propagation_time",
            ])?;
            csv_writer.flush()?;
        }

        Ok(Self {
            genesis_hash: config.genesis_hash,
            nodes_file: config.nodes_file,
            blocks_file: config.blocks_file,
            nodes: Arc::new(Mutex::new(nodes)),
            blocks: Arc::new(Mutex::new(blocks)),
            csv_writer: Arc::new(Mutex::new(csv_writer)),
        })
    }

    async fn process_message(&self, msg: &str) -> Result<()> {
        trace!("Processing message: {}", msg);
        let value: Value = serde_json::from_str(msg)?;

        if let Some(arr) = value.as_array() {
            if arr.is_empty() {
                trace!("Empty array message, ignoring");
                return Ok(());
            }

            trace!(
                "Message array length: {}, first element: {:?}",
                arr.len(),
                arr[0]
            );
            match arr[0].as_u64() {
                Some(3) => {
                    debug!("Processing node message (type 3)");
                    self.process_node_message(arr).await?
                }
                Some(6) => {
                    debug!("Processing block import message (type 6)");
                    self.process_block_import(arr).await?
                }
                Some(msg_type) => {
                    trace!("Ignoring message type: {}", msg_type);
                }
                None => {
                    warn!("First element is not a number: {:?}", arr[0]);
                }
            }
        } else {
            warn!("Message is not an array: {:?}", value);
        }

        Ok(())
    }

    async fn process_node_message(&self, arr: &[Value]) -> Result<()> {
        debug!(
            "process_node_message called with array length: {}",
            arr.len()
        );
        if arr.len() < 2 {
            debug!("Node message array too short, need at least 2 elements");
            return Ok(());
        }

        trace!("Node message structure: {:?}", arr);

        // The structure is [3, [node_idx, [node_data...]]]
        if let Some(node_array) = arr[1].as_array() {
            if node_array.len() >= 2 {
                if let (Some(node_idx), Some(node_data)) =
                    (node_array[0].as_u64(), node_array[1].as_array())
                {
                    debug!(
                        "Node idx: {}, node data array length: {}",
                        node_idx,
                        node_data.len()
                    );
                    if node_data.len() >= 5 {
                        let node_name = node_data[0].as_str().unwrap_or("unknown").to_string();
                        let node_id = if let Some(arr) = node_data[4].as_array() {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(",")
                        } else {
                            node_data[4].as_str().unwrap_or("unknown").to_string()
                        };

                        info!(
                            "Storing node: idx={}, name={}, id={}",
                            node_idx, node_name, node_id
                        );
                        let mut nodes = self.nodes.lock().await;
                        nodes.insert(
                            node_idx.to_string(),
                            NodeInfo {
                                name: node_name,
                                node_id,
                            },
                        );
                    }
                }
            }
        }

        // Save nodes to file
        self.save_nodes().await?;
        Ok(())
    }

    async fn process_block_import(&self, arr: &[Value]) -> Result<()> {
        trace!("process_block_import called with array: {:?}", arr);
        if arr.len() < 2 || !arr[1].is_array() {
            debug!(
                "Block import validation failed: arr.len()={}, arr[1].is_array()={}",
                arr.len(),
                arr.get(1).map_or(false, |v| v.is_array())
            );
            return Ok(());
        }

        let data = arr[1].as_array().unwrap();
        trace!("Block import data array: {:?}", data);
        if data.len() < 2 || !data[1].is_array() {
            debug!(
                "Block import inner validation failed: data.len()={}, data[1].is_array()={}",
                data.len(),
                data.get(1).map_or(false, |v| v.is_array())
            );
            return Ok(());
        }

        let node_idx = data[0].as_u64().unwrap_or(0);
        let block_data = data[1].as_array().unwrap();
        trace!("Node idx: {}, block_data: {:?}", node_idx, block_data);

        if block_data.len() < 5 {
            debug!("Block data too short: len={}", block_data.len());
            return Ok(());
        }

        let block_number = block_data[0].as_u64().unwrap_or(0);
        let block_hash = block_data[1].as_str().unwrap_or("").to_string();
        let propagation_time = block_data[4].as_u64().unwrap_or(0);

        debug!(
            "Block details: number={}, hash={}, prop_time={}",
            block_number, block_hash, propagation_time
        );

        if block_hash.is_empty() || propagation_time == 0 {
            debug!("Invalid block data: empty hash or zero prop time");
            return Ok(());
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let nodes = self.nodes.lock().await;
        debug!(
            "Looking up node idx {} in nodes map with {} entries",
            node_idx,
            nodes.len()
        );
        let node_info = nodes.get(&node_idx.to_string());
        let node_name = node_info
            .map(|n| n.name.clone())
            .unwrap_or_else(|| format!("unknown_node_{}", node_idx));
        let node_id = node_info
            .map(|n| n.node_id.clone())
            .unwrap_or_else(|| "unknown_id".to_string());
        debug!("Node lookup result: name={}, id={}", node_name, node_id);
        drop(nodes);

        let mut blocks = self.blocks.lock().await;
        let block = blocks.entry(block_hash.clone()).or_insert(BlockInfo {
            block_number,
            lowest_prop_time: 999999,
            reporters: vec![],
            first_seen: now,
            report_count: 0,
            output: false,
        });

        block.report_count += 1;

        if propagation_time < block.lowest_prop_time {
            block.lowest_prop_time = propagation_time;
            block.reporters = vec![BlockReporter {
                node_idx,
                node_name,
                node_id,
                timestamp: now,
            }];
        } else if propagation_time == block.lowest_prop_time {
            if !block.reporters.iter().any(|r| r.node_idx == node_idx) {
                block.reporters.push(BlockReporter {
                    node_idx,
                    node_name,
                    node_id,
                    timestamp: now,
                });
            }
        }

        // Check if any blocks are ready for output
        let max_block = blocks.values().map(|b| b.block_number).max().unwrap_or(0);
        debug!(
            "Checking blocks for output: current block={}, max_block={}, total blocks={}",
            block_number,
            max_block,
            blocks.len()
        );

        let mut outputs = vec![];
        for (hash, block) in blocks.iter_mut() {
            let time_since_first = now - block.first_seen;
            let should_output = !block.output
                && (block.report_count >= 3
                    || time_since_first > 3
                    || block.block_number < max_block.saturating_sub(1));

            if should_output {
                debug!(
                    "Block {} ready for output: report_count={}, time_since_first={}, block_num={}, max_block={}",
                    hash, block.report_count, time_since_first, block.block_number, max_block
                );
            }

            if should_output {
                for reporter in &block.reporters {
                    debug!(
                        "Adding output for block {}: node={}, prop_time={}",
                        block.block_number, reporter.node_name, block.lowest_prop_time
                    );
                    outputs.push((
                        reporter.timestamp,
                        reporter.node_name.clone(),
                        reporter.node_id.clone(),
                        block.block_number,
                        hash.clone(),
                        block.lowest_prop_time,
                    ));
                }
                block.output = true;
            }
        }

        debug!("Total outputs to write: {}", outputs.len());

        // Log tracking status
        let now_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let time_str = format!(
            "[{:02}:{:02}:{:02}]",
            (now_time / 3600) % 24,
            (now_time / 60) % 60,
            now_time % 60
        );
        info!(
            "{} Tracking {} blocks, {} outputs ready",
            time_str,
            blocks.len(),
            outputs.len()
        );

        // Clean up old blocks (keep only recent 100)
        let mut block_list: Vec<_> = blocks
            .iter()
            .map(|(k, v)| (k.clone(), v.block_number))
            .collect();
        block_list.sort_by_key(|(_, num)| std::cmp::Reverse(*num));
        if block_list.len() > 100 {
            for (hash, _) in &block_list[100..] {
                blocks.remove(hash);
            }
        }

        drop(blocks);

        // Write outputs to CSV
        if !outputs.is_empty() {
            info!("Writing {} records to CSV", outputs.len());
            let mut csv_writer = self.csv_writer.lock().await;
            for (timestamp, node_name, node_id, block_number, block_hash, prop_time) in outputs {
                debug!(
                    "CSV write: timestamp={}, node={}, block={}",
                    timestamp, node_name, block_number
                );
                csv_writer.write_record(&[
                    timestamp.to_string(),
                    node_name,
                    node_id,
                    block_number.to_string(),
                    block_hash,
                    prop_time.to_string(),
                ])?;
            }
            csv_writer.flush()?;
            debug!("CSV flush complete");
        }

        // Save blocks to file
        self.save_blocks().await?;

        // Log tracking info
        let blocks = self.blocks.lock().await;
        let block_count = blocks.len();
        let output_count = blocks.values().filter(|b| !b.output).count();
        drop(blocks);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let hours = (now % 86400) / 3600;
        let minutes = (now % 3600) / 60;
        let seconds = now % 60;
        info!(
            "[{:02}:{:02}:{:02}] Tracking {} blocks, {} outputs ready",
            hours, minutes, seconds, block_count, output_count
        );

        Ok(())
    }

    async fn save_nodes(&self) -> Result<()> {
        let nodes = self.nodes.lock().await;
        let file = File::create(&self.nodes_file)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &*nodes)?;
        Ok(())
    }

    async fn save_blocks(&self) -> Result<()> {
        let blocks = self.blocks.lock().await;
        let file = File::create(&self.blocks_file)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &*blocks)?;
        Ok(())
    }

    async fn run(&self, url: &str) -> Result<()> {
        debug!("run() method called with URL: {}", url);
        loop {
            debug!("Starting telemetry monitoring loop iteration...");
            info!("Starting telemetry monitoring...");
            debug!(
                "Connecting to {} with genesis hash {}",
                url, self.genesis_hash
            );

            // Parse the URL to http::Uri
            let uri: http::Uri = url.parse().expect("Invalid WebSocket URL");
            info!("Attempting WebSocket connection to: {}", uri);

            match ws_client::connect(&uri).await {
                Ok(connection) => {
                    info!("WebSocket connection established!");
                    let (sender, mut receiver) = connection.into_channels();

                    // Send subscription message
                    let subscribe_msg = format!("subscribe:{}", self.genesis_hash);
                    debug!("Sending subscription message: {}", subscribe_msg);
                    if let Err(e) =
                        sender.unbounded_send(SentMessage::Text(subscribe_msg.to_string()))
                    {
                        error!("Failed to send subscription: {}", e);
                        continue;
                    }
                    debug!("Subscription message sent successfully");

                    // Read messages
                    debug!("Starting message receive loop...");
                    loop {
                        trace!("Waiting for next message...");
                        match receiver.next().await {
                            Some(Ok(RecvMessage::Text(text))) => {
                                trace!("Received text message: {}", text);
                                debug!(
                                    "Received line: {}...",
                                    &text.chars().take(100).collect::<String>()
                                );
                                if let Err(e) = self.process_message(&text).await {
                                    warn!("Failed to process message: {}", e);
                                }
                            }
                            Some(Ok(RecvMessage::Binary(data))) => {
                                trace!("Received binary message, converting to text...");
                                match String::from_utf8(data) {
                                    Ok(text) => {
                                        trace!("Binary message converted to text: {}", text);
                                        debug!(
                                            "Received binary line: {}...",
                                            &text.chars().take(100).collect::<String>()
                                        );
                                        if let Err(e) = self.process_message(&text).await {
                                            error!("Failed to process binary message: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to convert binary message to UTF-8: {}", e);
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                error!("WebSocket error: {}", e);
                                break;
                            }
                            None => {
                                info!("WebSocket closed");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect: {}", e);
                    debug!("Sleeping for 5 seconds before retry...");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }

            info!("Connection lost or error occurred. Reconnecting in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    // Check for help flag
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        println!("Telemetry Observer - Monitor block production and propagation times");
        println!();
        println!("USAGE:");
        println!("    {} [OPTIONS]", args[0]);
        println!();
        println!("OPTIONS:");
        println!("    -h, --help              Print help information");
        println!(
            "    --genesis-hash <HASH>   Genesis hash to monitor (default: {})",
            "0x91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3"
        );
        println!("    --telemetry-url <URL>   Telemetry WebSocket URL (default: wss://telemetry.polkadot.io/feed/0)");
        return Ok(());
    }

    env_logger::init();
    debug!("Logger initialized");

    let mut config = Config::default();

    // Parse command line arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--genesis-hash" => {
                if i + 1 < args.len() {
                    config.genesis_hash = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --genesis-hash requires a value");
                    std::process::exit(1);
                }
            }
            "--telemetry-url" => {
                if i + 1 < args.len() {
                    config.telemetry_url = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --telemetry-url requires a value");
                    std::process::exit(1);
                }
            }
            _ => {
                eprintln!("Error: Unknown option '{}'", args[i]);
                eprintln!("Try '{} --help' for more information", args[0]);
                std::process::exit(1);
            }
        }
    }

    let url = config.telemetry_url.clone();
    info!(
        "Creating TelemetryObserver with URL: {} and genesis hash: {}",
        url, config.genesis_hash
    );
    let observer = TelemetryObserver::new(config).await?;
    info!("TelemetryObserver created, starting run loop...");
    observer.run(&url).await
}
