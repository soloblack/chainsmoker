/*
 More details: https://github.com/solana-foundation/specs/blob/main/p2p/shred.md

*  ** Common Header **
: The common header has size 0x53 (83 bytes).
! +--------+-----+-------------------+------------------+--------------------------------+
! | Offset | Size| Type              | Name             | Purpose                        |
! +--------+-----+-------------------+------------------+--------------------------------+
! | 0x00   | 64B | Ed25519 signature | signature        | Block producer signature       |
! | 0x40   |  1B | u8                | variant          | Shred variant                  |
! | 0x41   |  8B | u64               | slot             | Slot number                    |
! | 0x49   |  4B | u32               | shred_index      | Shred index                    |
! | 0x4d   |  2B | u16               | shred_version    | Shred version                  |
! | 0x4f   |  4B | u32               | fec_set_index    | FEC Set Index                  |
! +--------+-----+-------------------+------------------+--------------------------------+

*  ** Shred Variant Field **
: The shred variant identifies the shred type (data, code) and authentication mechanism (legacy, Merkle).
: The field is encoded as two 4-bit unsigned integers.
: The high 4-bit field is at bit range 4:8, and the low 4-bit field is at bit range 0:4.

! +------------+------------+--------------+-------------------+
! | High 4-bit | Low 4-bit  | Shred Type   | Authentication     |
! +------------+------------+--------------+-------------------+
! | 0x5        | 0xa        | Code         | Legacy             |
! | 0xa        | 0x5        | Data         | Legacy             |
! | 0x4        | Any        | Code         | Merkle             |
! | 0x8        | Any        | Data         | Merkle             |
! +------------+------------+--------------+-------------------+

*  ** Data Shred Header **
! +--------+-----+-------+----------------+--------------------------------+
! | Offset | Size| Type  | Name           | Purpose                        |
! +--------+-----+-------+----------------+--------------------------------+
! | 0x53   | 2B  | u16   | parent_offset  | Slot distance to parent block  |
! | 0x55   | 1B  | u8    | data_flags     | Data Flags                     |
! | 0x56   | 2B  | u16   | size           | Total Size                     |
! +--------+-----+-------+----------------+--------------------------------+


*  ** Code Shred Header **
! +--------+-----+-------+--------------------+-----------------------------------------+
! | Offset | Size| Type  | Name               | Purpose                                 |
! +--------+-----+-------+--------------------+-----------------------------------------+
! | 0x53   | 2B  | u16   | num_data_shreds    | Number of data shreds                   |
! | 0x55   | 2B  | u16   | num_coding_shreds  | Number of coding shreds                 |
! | 0x57   | 2B  | u16   | position           | Position of this shred in FEC set       |
! +--------+-----+-------+--------------------+-----------------------------------------+

*  ** Shred Packet Size **
: The maximum shred packet size is determined based on the IPv6 minimum link MTU.

! Max size for shred packet is 1228 bytes (Legacy) or 1203 bytes (Merkle).
*/

use crate::{deshred::DeshredManager, stats::ReceiveStats, utils::{get_timestamp_us, parse_shred}};
use log::{debug, error, info, warn};
use solana_ledger::shred::Shred;
use std::{net::{SocketAddr, UdpSocket}, sync::Arc, thread, time::Duration};
use std::sync::mpsc;

// Structure to carry shred with sender address
#[derive(Clone)]
pub struct ShredWithAddr {
    pub recv_ts_us: u64,
    pub shred: Shred,
    pub sender_addr: SocketAddr,
}

pub struct ShredReceiver {
    socket: Arc<UdpSocket>,
    sender: mpsc::Sender<ShredWithAddr>,
    receiver: Option<mpsc::Receiver<ShredWithAddr>>,
    shred_version: u16,
}

impl ShredReceiver {
    pub fn new(socket: Arc<UdpSocket>, shred_version: u16) -> Self {
        let (sender, receiver) = mpsc::channel::<ShredWithAddr>();

        if let Err(e) = socket.set_nonblocking(false) {
            error!("Failed to set socket blocking: {}", e);
        }

        Self {
            socket,
            sender,
            receiver: Some(receiver), 
            shred_version,
        }
    }

    fn process_packet(data: &[u8], sender_addr: SocketAddr, count: u64, recv_ts_us: u64) -> Option<ShredWithAddr> {
        match parse_shred(data) {
            Ok(shred) => {
                debug!(
                    "SHRED #{}: Slot:{} Index:{} Type:{:?} from {}",
                    count,
                    shred.slot(),
                    shred.index(),
                    shred.shred_type(),
                    sender_addr
                );

                Some(ShredWithAddr {
                    recv_ts_us,
                    shred,
                    sender_addr,
                })
            }
            Err(_) => {
                warn!(
                    "NON-SHRED #{}: {} bytes from {}",
                    count,
                    data.len(),
                    sender_addr
                );
                None
            }
        }
    }

    pub fn start(&mut self) -> thread::JoinHandle<()> {
        let socket = self.socket.clone();
        let sender = self.sender.clone();
        let shred_version = self.shred_version;
        thread::spawn(move || {
            info!("Starting shred receiver...");

            let mut buffer = [0u8; 1232];
            let mut stats = ReceiveStats::new();
            let mut deshred_manager = DeshredManager::new(shred_version);

            loop {
                match socket.recv_from(&mut buffer) {
                    Ok((size, sender_addr)) => {
                        let recv_ts_us = get_timestamp_us();
                        stats.increment();

                        if let Some(shred_data) =
                            Self::process_packet(&buffer[..size], sender_addr, stats.count, recv_ts_us)
                        {
                            // CHANGED: Use std::sync::mpsc send() instead of tokio
                            if let Some((slot, entries, payload)) = deshred_manager.add_shred(shred_data.shred) {
                                info!("Deshredded slot: {} entries: {} payload: {}", slot, entries.len(), payload.len());
                                for entry in entries {
                                    for tx in entry.transactions {
                                        info!("Transaction: {tx:?}");
                                    }
                                }
                            }
                        }

                        stats.maybe_log();
                    }
                    Err(e) => {
                        error!("Receive error: {}", e);
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        })
    }

    pub fn take_receiver(&mut self) -> mpsc::Receiver<ShredWithAddr> {
        self.receiver.take().expect("Receiver already taken")
    }
}
