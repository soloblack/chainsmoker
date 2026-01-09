use std::{
    env,
    net::{IpAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};

use chainsmoker::{
    Keypair, ShredWithAddr,
    gossip::GossipNode,
    output::{OutputPlugin, PluginRunner},
    shred::ShredReceiver,
    types::Network,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};
use log::{info, error};

// simple console plugin can be grpc/quinn but just console as example
struct ConsolePlugin {
    // Track which slots we've seen and who first sent them
    seen_slots: Arc<Mutex<HashSet<u64>>>,
    // Track first sender (IP and timestamp) for each slot
    slot_first_sender: Arc<Mutex<HashMap<u64, (std::net::IpAddr, u64)>>>,
    // Track statistics: count of new slots per IP address
    ip_stats: Arc<Mutex<HashMap<std::net::IpAddr, u64>>>,
}

impl ConsolePlugin {
    fn new() -> Self {
        Self {
            seen_slots: Arc::new(Mutex::new(HashSet::new())),
            slot_first_sender: Arc::new(Mutex::new(HashMap::new())),
            ip_stats: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl OutputPlugin for ConsolePlugin {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Console plugin started");
        Ok(())
    }

    async fn handle_shred(&mut self, shred_with_addr: ShredWithAddr) -> Result<(), Box<dyn std::error::Error>> {
        let shred = &shred_with_addr.shred;
        let sender_addr = shred_with_addr.sender_addr;
        let sender_ip = sender_addr.ip(); // Extract IP address, remove port
        let slot = shred.slot();
        let recv_ts_us = shred_with_addr.recv_ts_us;

        // Check if this is a new slot
        let is_new_slot = {
            let mut seen_slots = self.seen_slots.lock().unwrap();
            seen_slots.insert(slot)
        };

        if is_new_slot {
            // Record the first sender (IP and timestamp) for this slot
            {
                let mut slot_first_sender = self.slot_first_sender.lock().unwrap();
                slot_first_sender.insert(slot, (sender_ip, recv_ts_us));
            }

            // Update statistics for this IP address
            {
                let mut ip_stats = self.ip_stats.lock().unwrap();
                *ip_stats.entry(sender_ip).or_insert(0) += 1;
            }

            // Log top 2 IP addresses with total slot count and time difference
            {
                let seen_slots = self.seen_slots.lock().unwrap();
                let total_slots = seen_slots.len();
                drop(seen_slots); // Release lock early
                
                let ip_stats = self.ip_stats.lock().unwrap();
                let mut sorted_stats: Vec<(&std::net::IpAddr, &u64)> = ip_stats.iter().collect();
                sorted_stats.sort_by(|a, b| b.1.cmp(a.1));
                
                let mut log_msg = format!(
                    "NEW SLOT {} first received from {} | Total new slots: {} | Top 2 IP addresses:",
                    slot,
                    sender_ip,
                    total_slots
                );
                
                // Display top 2 IPs with their counts
                for (i, (ip, count)) in sorted_stats.iter().take(2).enumerate() {
                    log_msg.push_str(&format!(" {}. {}: {} new slots", i + 1, ip, count));
                }
                
                // Calculate and display time difference between top 2 if both exist
                if sorted_stats.len() >= 2 {
                    let top1_ip = sorted_stats[0].0;
                    let top2_ip = sorted_stats[1].0;
                    
                    // Find the earliest timestamp for each IP from slot_first_sender
                    let slot_first_sender = self.slot_first_sender.lock().unwrap();
                    let mut top1_earliest: Option<u64> = None;
                    let mut top2_earliest: Option<u64> = None;
                    
                    for (_, (ip, ts)) in slot_first_sender.iter() {
                        if ip == top1_ip {
                            top1_earliest = Some(top1_earliest.map_or(*ts, |e| e.min(*ts)));
                        } else if ip == top2_ip {
                            top2_earliest = Some(top2_earliest.map_or(*ts, |e| e.min(*ts)));
                        }
                    }
                    
                    if let (Some(ts1), Some(ts2)) = (top1_earliest, top2_earliest) {
                        let diff_us = ts1.abs_diff(ts2);
                        log_msg.push_str(&format!(" | Time difference between top 2: {} us", diff_us));
                    }
                }
                
                info!("{}", log_msg);
            }
        }

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Console plugin stopped");
        Ok(())
    }

    fn name(&self) -> &str {
        "Console"
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("chainsmoker=info,solana_gossip=warn,solana_metrics=error");

    // Read configuration from environment variables
    let bind_address: IpAddr = env::var("BIND_ADDRESS")
        .map_err(|_| "BIND_ADDRESS environment variable is required")?
        .parse()
        .map_err(|e| format!("Invalid BIND_ADDRESS: {}", e))?;

    let gossip_port: u16 = env::var("GOSSIP_PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse()
        .map_err(|e| format!("Invalid GOSSIP_PORT: {}", e))?;

    let tvu_port: u16 = env::var("TVU_PORT")
        .unwrap_or_else(|_| "8001".to_string())
        .parse()
        .map_err(|e| format!("Invalid TVU_PORT: {}", e))?;

    // Check if gossip service should be enabled
    let enable_gossip = env::var("ENABLE_GOSSIP")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    info!("Configuration:");
    info!("  BIND_ADDRESS: {}", bind_address);
    info!("  GOSSIP_PORT: {}", gossip_port);
    info!("  TVU_PORT: {}", tvu_port);
    info!("  ENABLE_GOSSIP: {}", enable_gossip);

    let identity_keypair = Arc::new(Keypair::new());

    let tvu_socket = UdpSocket::bind((bind_address, tvu_port))?;

    // Only start gossip service if enabled
    if enable_gossip {
        let gossip_socket = UdpSocket::bind((bind_address, gossip_port))?;
        let gossip_node = GossipNode::new(
            identity_keypair,
            gossip_socket,
            &tvu_socket,
            bind_address,
            Network::Mainnet,
        )?;

        gossip_node.start_discovery(); // breaks when peers > 100
        info!("Finished gossip discovery");
    } else {
        info!("Gossip service disabled, skipping discovery");
    }

    let mut shred_receiver = ShredReceiver::new(Arc::new(tvu_socket));

    // get the receiver BEFORE starting the sender thread to prevent race condition
    let receiver = shred_receiver.take_receiver();
    let _shred_handle = shred_receiver.start(); // Start receiving

    let mut plugin_runner = PluginRunner::new();
    plugin_runner.add_plugin(Box::new(ConsolePlugin::new()));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        plugin_runner.start_all().await.unwrap();

        let receiver = std::sync::Arc::new(std::sync::Mutex::new(receiver));

        loop {
            let receiver_clone = receiver.clone();

            let shred_result = tokio::task::spawn_blocking(move || {
                let receiver = receiver_clone.lock().unwrap();
                receiver.recv_timeout(Duration::from_secs(1))
            })
            .await;

            match shred_result {
                Ok(Ok(shred_with_addr)) => {
                    plugin_runner.handle_shred(shred_with_addr).await;
                }
                Ok(Err(std::sync::mpsc::RecvTimeoutError::Timeout)) => {
                    continue;
                }
                Ok(Err(std::sync::mpsc::RecvTimeoutError::Disconnected)) => {
                    error!("Shred receiver channel disconnected");
                    break;
                }
                Err(_) => {
                    error!("Error in shred receiver task");
                    break;
                }
            }
        }

        plugin_runner.stop_all().await.unwrap();
    });

    Ok(())
}
