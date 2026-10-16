use std::io;

const MAX_BLOCK: usize = 64 * 1024;
// snap 1.1.x max_compress_len(64 KiB) == 76_490.
const MAX_COMPRESSED_CHUNK: usize = 4 + 76_490;
/// Bytes we may keep buffered between feeds. The parse loop only ever leaves an
/// incomplete chunk (or a partial stream identifier) behind, and `/debug`-style
/// per-chunk length checks bound that at one header plus the largest legal body.
const MAX_RETAINED_INPUT: usize = 4 + MAX_COMPRESSED_CHUNK;
/// Bound the decoder's internal input buffer even when a public caller passes
/// a larger slice. Only one incomplete data chunk is retained across slices.
const FEED_SLICE: usize = 64 * 1024;

fn masked_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
        .rotate_right(15)
        .wrapping_add(0xa282_ead8)
}

fn check_checksum(chunk: &[u8], decoded: &[u8]) -> io::Result<()> {
    let expected = u32::from_le_bytes(chunk[..4].try_into().unwrap());
    if masked_crc32c(decoded) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Snappy chunk checksum mismatch",
        ));
    }
    Ok(())
}

/// A persistent Snappy framing stream decoder that maintains state across
/// incremental data feeds. This is required because KCP stream mode can
/// split snappy frames across multiple recv() calls, and the snappy
/// framing format's stream header (0xff...) only appears once at the start.
pub struct SnappyStreamDecoder {
    buf: Vec<u8>,
    pos: usize,
    hdr_ok: bool,
    /// Bytes left in a skippable chunk; do not buffer its potentially 16 MB body.
    skip_remaining: usize,
    /// Reused across chunks — Decoder::new() is cheap but not free under
    /// high-throughput session-level compression.
    decoder: snap::raw::Decoder,
}

impl Default for SnappyStreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl SnappyStreamDecoder {
    pub fn new() -> Self {
        SnappyStreamDecoder {
            buf: Vec::new(),
            pos: 0,
            hdr_ok: false,
            skip_remaining: 0,
            decoder: snap::raw::Decoder::new(),
        }
    }
    pub fn feed(&mut self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.feed_chunks(data, |chunk| out.extend_from_slice(chunk))?;
        Ok(out)
    }

    /// Decode complete chunks into a sink so transport readers do not have to
    /// collect a whole read's expanded output before passing it to SMUX.
    pub(crate) fn feed_chunks(
        &mut self,
        data: &[u8],
        mut emit: impl FnMut(&[u8]),
    ) -> io::Result<()> {
        if data.is_empty() {
            return self.feed_slice(&[], &mut emit);
        }
        for slice in data.chunks(FEED_SLICE) {
            self.feed_slice(slice, &mut emit)?;
        }
        Ok(())
    }

    fn feed_slice(&mut self, data: &[u8], emit: &mut impl FnMut(&[u8])) -> io::Result<()> {
        if self.pos > 65536 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
        loop {
            let avail = self.buf.len() - self.pos;
            if self.skip_remaining > 0 {
                let skipped = avail.min(self.skip_remaining);
                self.pos += skipped;
                self.skip_remaining -= skipped;
                if self.skip_remaining > 0 {
                    break;
                }
                continue;
            }
            // Skip stream identifier (0xFF 0x06 0x00 0x00 "sNaPpY")
            if !self.hdr_ok {
                if avail < 10 {
                    break;
                }
                if self.buf[self.pos] != 0xff || &self.buf[self.pos + 4..self.pos + 10] != b"sNaPpY"
                {
                    // Not a stream identifier — skip one byte and try to resync
                    self.pos += 1;
                    continue;
                }
                self.pos += 10;
                self.hdr_ok = true;
                continue;
            }
            // Read chunk header: [type 1B][length 3B LE]
            if avail < 4 {
                break;
            }
            let ct = self.buf[self.pos];
            let chunk_len = u32::from_le_bytes([
                self.buf[self.pos + 1],
                self.buf[self.pos + 2],
                self.buf[self.pos + 3],
                0,
            ]) as usize;
            if (0x80..=0xfe).contains(&ct) {
                self.pos += 4;
                self.skip_remaining = chunk_len;
                continue;
            }
            let limit = match ct {
                0x00 => MAX_COMPRESSED_CHUNK,
                0x01 => MAX_BLOCK + 4,
                0xff => 6,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported Snappy chunk type",
                    ));
                }
            };
            if chunk_len > limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Snappy chunk exceeds its maximum length",
                ));
            }
            if 4 + chunk_len > avail {
                break;
            }
            let chunk_data = &self.buf[self.pos + 4..self.pos + 4 + chunk_len];
            self.pos += 4 + chunk_len;
            match ct {
                0x00 => {
                    // Compressed chunk: [CRC32 4B][snappy block]
                    if chunk_data.len() < 4 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Snappy compressed chunk is missing its checksum",
                        ));
                    }
                    let snappy_data = &chunk_data[4..];
                    let decoded_len = snap::raw::decompress_len(snappy_data)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    if decoded_len > MAX_BLOCK {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Snappy decoded block exceeds its limit",
                        ));
                    }
                    let decoded = self
                        .decoder
                        .decompress_vec(snappy_data)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    check_checksum(chunk_data, &decoded)?;
                    emit(&decoded);
                }
                0x01 if chunk_data.len() >= 4 => {
                    // Uncompressed chunk: [CRC32 4B][raw data]
                    let decoded = &chunk_data[4..];
                    check_checksum(chunk_data, decoded)?;
                    emit(decoded);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Snappy uncompressed chunk is missing its checksum",
                    ));
                }
            }
        }
        if self.buf.len() - self.pos > MAX_RETAINED_INPUT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Snappy input exceeds the decoder buffer limit",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
