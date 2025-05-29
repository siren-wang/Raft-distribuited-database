"""
Write-Ahead Log (WAL) Implementation for Raft
Provides durable storage for Raft log entries with crash recovery
"""

import os
import json
import struct
import asyncio
import aiofiles
import aiofiles.os
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
import hashlib

logger = logging.getLogger(__name__)


class WALEntry:
    """Represents a single entry in the WAL"""
    
    # Entry format: [checksum(4)] [length(4)] [term(8)] [index(8)] [timestamp(8)] [data(variable)]
    HEADER_SIZE = 32  # 4 + 4 + 8 + 8 + 8 bytes
    
    def __init__(self, term: int, index: int, command: Dict[str, Any], timestamp: Optional[datetime] = None):
        self.term = term
        self.index = index
        self.command = command
        self.timestamp = timestamp or datetime.utcnow()
    
    def serialize(self) -> bytes:
        """Serialize entry to bytes for storage"""
        # Serialize command to JSON
        command_json = json.dumps(self.command).encode('utf-8')
        
        # Pack header (without checksum and length)
        header = struct.pack(
            '>QQQ',  # Big-endian: term(8), index(8), timestamp(8)
            self.term,
            self.index,
            int(self.timestamp.timestamp() * 1000000)  # Microseconds since epoch
        )
        
        # Combine header and data
        data = header + command_json
        
        # Calculate checksum
        checksum = self._calculate_checksum(data)
        
        # Pack final entry with checksum and length
        entry = struct.pack('>II', checksum, len(data)) + data
        
        return entry
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'WALEntry':
        """Deserialize entry from bytes"""
        # Unpack header
        checksum, length = struct.unpack('>II', data[:8])
        entry_data = data[8:8+length]
        
        # Verify checksum
        calculated_checksum = cls._calculate_checksum(entry_data)
        if checksum != calculated_checksum:
            raise ValueError(f"Checksum mismatch: expected {checksum}, got {calculated_checksum}")
        
        # Unpack entry header
        term, index, timestamp_us = struct.unpack('>QQQ', entry_data[:24])
        
        # Parse command JSON
        command_json = entry_data[24:].decode('utf-8')
        command = json.loads(command_json)
        
        # Convert timestamp
        timestamp = datetime.fromtimestamp(timestamp_us / 1000000)
        
        return cls(term=term, index=index, command=command, timestamp=timestamp)
    
    @staticmethod
    def _calculate_checksum(data: bytes) -> int:
        """Calculate CRC32 checksum for data"""
        return hashlib.crc32(data) & 0xffffffff
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp.isoformat()
        }


class WriteAheadLog:
    """
    Write-Ahead Log implementation for durable Raft log storage
    """
    
    def __init__(self, node_id: str, wal_dir: str = "./raft_wal"):
        self.node_id = node_id
        self.wal_dir = Path(wal_dir) / node_id
        self.wal_dir.mkdir(parents=True, exist_ok=True)
        
        # WAL file management
        self.current_segment = 0
        self.segment_size = 10 * 1024 * 1024  # 10MB segments
        self.current_file: Optional[aiofiles.threadpool.binary.AsyncBufferedIOBase] = None
        self.current_file_size = 0
        
        # In-memory index for fast lookups
        self.index: Dict[int, tuple[int, int]] = {}  # log_index -> (segment, offset)
        
        # Write lock
        self._write_lock = asyncio.Lock()
        
        logger.info(f"Initialized WAL for node {node_id} at {self.wal_dir}")
    
    async def initialize(self):
        """Initialize WAL and recover from existing segments"""
        await self._recover_from_disk()
        await self._open_segment(self.current_segment)
    
    async def close(self):
        """Close WAL files"""
        if self.current_file:
            await self.current_file.close()
            self.current_file = None
    
    async def append(self, entry: WALEntry) -> int:
        """
        Append an entry to the WAL
        
        Returns:
            Offset where entry was written
        """
        async with self._write_lock:
            # Check if we need to rotate segment
            if self.current_file_size >= self.segment_size:
                await self._rotate_segment()
            
            # Serialize entry
            entry_bytes = entry.serialize()
            
            # Write to current segment
            offset = self.current_file_size
            await self.current_file.write(entry_bytes)
            await self.current_file.flush()
            
            # Update index
            self.index[entry.index] = (self.current_segment, offset)
            self.current_file_size += len(entry_bytes)
            
            logger.debug(f"Appended entry {entry.index} to WAL segment {self.current_segment} at offset {offset}")
            
            return offset
    
    async def read_entry(self, log_index: int) -> Optional[WALEntry]:
        """Read a specific entry by log index"""
        if log_index not in self.index:
            return None
        
        segment, offset = self.index[log_index]
        
        # Read from appropriate segment
        segment_path = self._get_segment_path(segment)
        
        try:
            async with aiofiles.open(segment_path, 'rb') as f:
                await f.seek(offset)
                
                # Read header to get length
                header = await f.read(8)
                if len(header) < 8:
                    logger.error(f"Incomplete header at offset {offset}")
                    return None
                
                checksum, length = struct.unpack('>II', header)
                
                # Read full entry
                entry_data = await f.read(length)
                if len(entry_data) < length:
                    logger.error(f"Incomplete entry at offset {offset}")
                    return None
                
                # Deserialize
                full_data = header + entry_data
                return WALEntry.deserialize(full_data)
                
        except Exception as e:
            logger.error(f"Error reading entry {log_index}: {e}")
            return None
    
    async def read_entries_from(self, start_index: int) -> List[WALEntry]:
        """Read all entries starting from the given index"""
        entries = []
        
        # Find all indices >= start_index
        indices = sorted([idx for idx in self.index.keys() if idx >= start_index])
        
        for idx in indices:
            entry = await self.read_entry(idx)
            if entry:
                entries.append(entry)
        
        return entries
    
    async def truncate_after(self, log_index: int):
        """Truncate all entries after the given index"""
        async with self._write_lock:
            # Find entries to remove
            indices_to_remove = [idx for idx in self.index.keys() if idx > log_index]
            
            if not indices_to_remove:
                return
            
            logger.info(f"Truncating {len(indices_to_remove)} entries after index {log_index}")
            
            # Remove from index
            for idx in indices_to_remove:
                del self.index[idx]
            
            # TODO: Implement actual file truncation for space reclamation
            # For now, entries are just removed from the index
    
    async def compact(self, up_to_index: int, snapshot_data: Optional[bytes] = None):
        """
        Compact the log up to the given index
        
        Args:
            up_to_index: Remove all entries up to this index
            snapshot_data: Optional snapshot data to store
        """
        async with self._write_lock:
            # Remove old entries from index
            indices_to_remove = [idx for idx in self.index.keys() if idx <= up_to_index]
            
            for idx in indices_to_remove:
                del self.index[idx]
            
            # Save snapshot if provided
            if snapshot_data:
                snapshot_path = self.wal_dir / f"snapshot_{up_to_index}.bin"
                async with aiofiles.open(snapshot_path, 'wb') as f:
                    await f.write(snapshot_data)
            
            logger.info(f"Compacted log up to index {up_to_index}, removed {len(indices_to_remove)} entries")
    
    async def _recover_from_disk(self):
        """Recover WAL state from existing segments"""
        logger.info("Recovering WAL from disk...")
        
        # Find all segment files
        segment_files = sorted(self.wal_dir.glob("segment_*.wal"))
        
        for segment_file in segment_files:
            segment_num = int(segment_file.stem.split('_')[1])
            await self._recover_segment(segment_num)
        
        # Set current segment
        if segment_files:
            self.current_segment = int(segment_files[-1].stem.split('_')[1])
            
            # Get size of current segment
            self.current_file_size = segment_files[-1].stat().st_size
        else:
            self.current_segment = 0
            self.current_file_size = 0
        
        logger.info(f"Recovered {len(self.index)} entries from {len(segment_files)} segments")
    
    async def _recover_segment(self, segment_num: int):
        """Recover entries from a specific segment"""
        segment_path = self._get_segment_path(segment_num)
        
        try:
            async with aiofiles.open(segment_path, 'rb') as f:
                offset = 0
                
                while True:
                    # Read header
                    header = await f.read(8)
                    if len(header) < 8:
                        break
                    
                    checksum, length = struct.unpack('>II', header)
                    
                    # Read entry data
                    entry_data = await f.read(length)
                    if len(entry_data) < length:
                        logger.warning(f"Incomplete entry at offset {offset} in segment {segment_num}")
                        break
                    
                    try:
                        # Deserialize entry
                        full_data = header + entry_data
                        entry = WALEntry.deserialize(full_data)
                        
                        # Add to index
                        self.index[entry.index] = (segment_num, offset)
                        
                        offset += 8 + length
                        
                    except Exception as e:
                        logger.error(f"Error deserializing entry at offset {offset}: {e}")
                        break
                        
        except Exception as e:
            logger.error(f"Error recovering segment {segment_num}: {e}")
    
    async def _open_segment(self, segment_num: int):
        """Open a segment file for writing"""
        if self.current_file:
            await self.current_file.close()
        
        segment_path = self._get_segment_path(segment_num)
        self.current_file = await aiofiles.open(segment_path, 'ab')
        self.current_segment = segment_num
        
        # Get current file size
        stat = await aiofiles.os.stat(segment_path)
        self.current_file_size = stat.st_size
        
        logger.info(f"Opened WAL segment {segment_num} (size: {self.current_file_size})")
    
    async def _rotate_segment(self):
        """Rotate to a new segment file"""
        await self._open_segment(self.current_segment + 1)
        self.current_file_size = 0
    
    def _get_segment_path(self, segment_num: int) -> Path:
        """Get path for a segment file"""
        return self.wal_dir / f"segment_{segment_num:06d}.wal"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics"""
        return {
            "node_id": self.node_id,
            "current_segment": self.current_segment,
            "current_file_size": self.current_file_size,
            "total_entries": len(self.index),
            "segments": len(list(self.wal_dir.glob("segment_*.wal"))),
            "wal_directory": str(self.wal_dir)
        }