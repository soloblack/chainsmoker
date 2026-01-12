
use {
    solana_ledger::shred::{Shred, ShredType},
    solana_sdk::clock::Slot,
    std::{
        sync::atomic::{AtomicU64, Ordering},
    },
};

const SLOT_WINDOW_SIZE: usize = 128;
const MAX_SHREDS_PER_SLOT: usize = 512; // keep your cap


pub struct ShrdsCompact {
    pub slot: Slot,
    pub shred_version: u16,
    // use bitset for tracking received shreds (8 u64s = 512 bits)
    received_mask: [u64; MAX_SHREDS_PER_SLOT / 64],
    // store only received shreds in a vec
    shreds: Vec<Option<Shred>>,
    // track segment boundaries
    segment_ends: Vec<u32>, // indices of DataComplete shreds
    last_processed: u32,
}


impl ShrdsCompact {
    pub fn new(slot: Slot, shred_version: u16) -> Self {
        Self {
            slot,
            shred_version,
            received_mask: [0; MAX_SHREDS_PER_SLOT / 64],
            shreds: Vec::with_capacity(128),
            segment_ends: Vec::with_capacity(4),
            last_processed: 0,
        }
    }

    #[inline]
    pub fn add_shred(&mut self, shred: Shred) -> bool {
        // data-only
        if shred.shred_type() != ShredType::Data {
            return false;
        }
        // don't mix shred_version
        if shred.version() != self.shred_version {
            return false;
        }

        let index = shred.index() as usize;
        if index >= MAX_SHREDS_PER_SLOT {
            return false;
        }

        let word_idx = index / 64;
        let bit_idx = index % 64;
        let mask = 1u64 << bit_idx;

        if self.received_mask[word_idx] & mask != 0 {
            return false;
        }

        // mark received
        self.received_mask[word_idx] |= mask;

        // segment boundary: only data_complete is a valid end for Shredder::deshred
        if shred.data_complete() {
            self.segment_ends.push(index as u32);
        }

        // ensure vec is large enough
        if index >= self.shreds.len() {
            self.shreds.resize(index + 1, None);
        }
        self.shreds[index] = Some(shred);

        true
    }
    #[inline]
    pub fn try_deshred_fast(&mut self) -> Option<(Vec<solana_entry::entry::Entry>, Vec<u8>)> {
        // check if we have any complete segments
        if self.segment_ends.is_empty() {
            return None;
        }

        // get next segment end
        let end_idx = self.segment_ends[0] as usize;
        let start_idx = self.last_processed as usize;

        // check if all shreds in segment are present (using bitset)
        for idx in start_idx..=end_idx {
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            if self.received_mask[word_idx] & (1u64 << bit_idx) == 0 {
                return None; // Missing shred
            }
        }

        // all present, deshred
        let shreds = &self.shreds[start_idx..=end_idx];

        // collect payloads
        let payloads: Vec<_> = shreds
            .iter()
            .filter_map(|s| s.as_ref().map(|shred| shred.payload()))
            .collect();

        if let Ok(deshredded) = solana_ledger::shred::Shredder::deshred(payloads) {
            // replace with wincode -> https://crates.io/crates/wincode
            if let Ok(entries) =
                bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&deshredded)
            {
                // mark segment as processed
                self.segment_ends.remove(0);
                self.last_processed = (end_idx + 1) as u32;

                // clear processed shreds to save memory
                for idx in start_idx..=end_idx {
                    self.shreds[idx] = None;
                }

                return Some((entries, deshredded));
            }
        }

        None
    }
}

pub struct DeshredManager {
    // use fixed-size array indexed by slot % WINDOW_SIZE
    slots: [Option<ShrdsCompact>; SLOT_WINDOW_SIZE],
    current_slot: AtomicU64,
}


impl DeshredManager {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            current_slot: AtomicU64::new(0),
        }
    }

    /// add shred without any locking
    #[inline]
    pub fn add_shred(
        &mut self,
        shred: Shred,
    ) -> Option<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)> {
        let slot = shred.slot();
        let slot_idx = (slot as usize) % SLOT_WINDOW_SIZE;

        // update current slot
        self.current_slot.store(slot, Ordering::Relaxed);

        // get or create slot entry
        let slot_shreds = match &mut self.slots[slot_idx] {
            Some(s) if s.slot == slot => s,
            slot_entry => {
                // replace with new slot
                *slot_entry = Some(ShrdsCompact::new(slot, shred.version()));
                slot_entry.as_mut().unwrap()
            }
        };

        if !slot_shreds.add_shred(shred) {
            return None; // duplicate
        }

        // try to deshred
        slot_shreds
            .try_deshred_fast()
            .map(|(entries, payload)| (slot, entries, payload))
    }

    /// cleanup old slots (using slot window)
    #[inline]
    pub fn cleanup_old_slots(&mut self, current_slot: Slot) {
        let threshold = current_slot.saturating_sub(SLOT_WINDOW_SIZE as u64);

        for slot_opt in &mut self.slots {
            if let Some(slot_shreds) = slot_opt {
                if slot_shreds.slot < threshold {
                    *slot_opt = None;
                }
            }
        }
    }
}