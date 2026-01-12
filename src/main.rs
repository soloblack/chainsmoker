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

const SHRED_VERSION: u16 = 50093;
// simple console plugin can be grpc/quinn but just console as example
struct ConsolePlugin {
    // Track which slots we've seen and who first sent them
    seen_slots: Arc<Mutex<HashSet<u64>>>,
    // Track all senders (IP and timestamp) for each slot
    slot_senders: Arc<Mutex<HashMap<u64, Vec<(std::net::IpAddr, u64)>>>>,
    // Track statistics: count of new slots per IP address
    ip_stats: Arc<Mutex<HashMap<std::net::IpAddr, u64>>>,
    // Track time differences: (total_diff_us, count) for average calculation
    time_diff_stats: Arc<Mutex<(u64, u64)>>, // (total_diff_us, count)
    // Track which slots we've already calculated time difference for
    calculated_slots: Arc<Mutex<HashSet<u64>>>,
}

impl ConsolePlugin {
    fn new() -> Self {
        Self {
            seen_slots: Arc::new(Mutex::new(HashSet::new())),
            slot_senders: Arc::new(Mutex::new(HashMap::new())),
            ip_stats: Arc::new(Mutex::new(HashMap::new())),
            time_diff_stats: Arc::new(Mutex::new((0, 0))),
            calculated_slots: Arc::new(Mutex::new(HashSet::new())),
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

        // Always record the sender and timestamp for this slot
        // For each IP, only keep the earliest timestamp
        let is_first_sender = {
            let mut slot_senders = self.slot_senders.lock().unwrap();
            let senders = slot_senders.entry(slot).or_insert_with(Vec::new);
            // Check if this IP already sent this slot
            let existing_index = senders.iter().position(|(ip, _)| *ip == sender_ip);
            if let Some(idx) = existing_index {
                // Update to keep the earliest timestamp
                if recv_ts_us < senders[idx].1 {
                    senders[idx].1 = recv_ts_us;
                }
                false
            } else {
                senders.push((sender_ip, recv_ts_us));
                senders.len() == 1 // First sender for this slot
            }
        };

        if is_new_slot {
            // Update statistics for this IP address (only count as "new slot" for first sender)
            {
                let mut ip_stats = self.ip_stats.lock().unwrap();
                *ip_stats.entry(sender_ip).or_insert(0) += 1;
            }
        }

        // Calculate time difference when we have at least 2 senders for this slot (only once per slot)
        {
            let mut calculated_slots = self.calculated_slots.lock().unwrap();
            if !calculated_slots.contains(&slot) {
                let slot_senders = self.slot_senders.lock().unwrap();
                if let Some(senders) = slot_senders.get(&slot) {
                    if senders.len() >= 2 {
                        // Sort by timestamp to find first and second
                        let mut sorted_senders = senders.clone();
                        sorted_senders.sort_by_key(|(_, ts)| *ts);
                        
                        let first_ts = sorted_senders[0].1;
                        let second_ts = sorted_senders[1].1;
                        let diff_us = second_ts - first_ts; // Time difference in microseconds
                        
                        // Mark this slot as calculated
                        calculated_slots.insert(slot);
                        drop(slot_senders); // Release lock before acquiring another
                        
                        // Update average statistics
                        {
                            let mut time_diff_stats = self.time_diff_stats.lock().unwrap();
                            time_diff_stats.0 += diff_us;
                            time_diff_stats.1 += 1;
                        }
                    }
                }
            }
        }

        // Log when we have a new slot or when we get a second sender
        {
            let slot_senders = self.slot_senders.lock().unwrap();
            if let Some(senders) = slot_senders.get(&slot) {
                if is_new_slot || senders.len() == 2 {
                    let seen_slots = self.seen_slots.lock().unwrap();
                    let total_slots = seen_slots.len();
                    drop(seen_slots); // Release lock early
                    
                    let ip_stats = self.ip_stats.lock().unwrap();
                    let mut sorted_stats: Vec<(&std::net::IpAddr, &u64)> = ip_stats.iter().collect();
                    sorted_stats.sort_by(|a, b| b.1.cmp(a.1));
                    
                    let mut log_msg = format!(
                        "SLOT {} received from {} | Total slots: {} | Senders for this slot: {} | Top 2 IP addresses:",
                        slot,
                        sender_ip,
                        total_slots,
                        senders.len()
                    );
                    
                    // Display top 2 IPs with their counts
                    for (i, (ip, count)) in sorted_stats.iter().take(2).enumerate() {
                        log_msg.push_str(&format!(" {}. {}: {} new slots", i + 1, ip, count));
                    }
                    
                    // Calculate and display time difference if we have at least 2 senders
                    if senders.len() >= 2 {
                        let mut sorted_senders = senders.clone();
                        sorted_senders.sort_by_key(|(_, ts)| *ts);
                        
                        let first_ip = sorted_senders[0].0;
                        let first_ts = sorted_senders[0].1;
                        let second_ip = sorted_senders[1].0;
                        let second_ts = sorted_senders[1].1;
                        let diff_us = second_ts - first_ts;
                        
                        log_msg.push_str(&format!(
                            " | Time diff ({} -> {}): {} us",
                            first_ip, second_ip, diff_us
                        ));
                        
                        // Display average time difference
                        let time_diff_stats = self.time_diff_stats.lock().unwrap();
                        if time_diff_stats.1 > 0 {
                            let avg_diff_us = time_diff_stats.0 / time_diff_stats.1;
                            log_msg.push_str(&format!(" | Avg time diff (1st->2nd): {} us (from {} slots)", avg_diff_us, time_diff_stats.1));
                        }
                    }
                    
                    info!("{}", log_msg);
                }
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

    let mut shred_receiver = ShredReceiver::new(Arc::new(tvu_socket), SHRED_VERSION);

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
