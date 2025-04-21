# bitcask_engine.py

import os
import random
import struct
from typing import Dict

SEGMENT_SIZE_LIMIT = 1024  # 1MB for demo purposes
TOMBSTONE = b"__tombstone__"


class Segment:
    def __init__(self, directory: str, segment_id: int):
        self.filename = os.path.join(directory, f"segment-{segment_id:06d}.db")
        self.file = open(self.filename, 'ab+')
        self.file.seek(0, os.SEEK_END)

    def write(self, key: bytes, value: bytes):
        pos = self.file.tell()
        record = struct.pack('>II', len(key), len(value)) + key + value
        self.file.write(record)
        self.file.flush()
        return pos

    def read_at(self, pos: int):
        with open(self.filename, 'rb') as f:
            f.seek(pos)
            key_len, val_len = struct.unpack('>II', f.read(8))
            key = f.read(key_len)
            value = f.read(val_len)
            return key, value

    def size(self):
        return os.path.getsize(self.filename)

    def close(self):
        self.file.close()


class Bitcask:
    def __init__(self, directory='data'):
        os.makedirs(directory, exist_ok=True)
        self.directory = directory
        self.segment_id = self._get_next_segment_id()
        self.active_segment = Segment(directory, self.segment_id)
        self.index: Dict[bytes, (str, int)] = {}
        self._load_index()

    def _get_next_segment_id(self):
        existing = [int(f.split('-')[1].split('.')[0]) for f in os.listdir(self.directory) if f.startswith('segment-')]
        return max(existing, default=0) + 1

    def _load_index(self):
        segments = sorted([f for f in os.listdir(self.directory) if f.startswith('segment-')])
        for segment in segments:
            path = os.path.join(self.directory, segment)
            with open(path, 'rb') as f:
                pos = 0
                while header := f.read(8):
                    key_len, val_len = struct.unpack('>II', header)
                    key = f.read(key_len)
                    value = f.read(val_len)
                    if value != TOMBSTONE:
                        self.index[key] = (segment, pos)
                    elif key in self.index:
                        del self.index[key]
                    pos = f.tell()

    def put(self, key: str, value: str):
        key_b = key.encode()
        value_b = value.encode()
        if self.active_segment.size() > SEGMENT_SIZE_LIMIT:
            self.active_segment.close()
            self.segment_id += 1
            self.active_segment = Segment(self.directory, self.segment_id)

        pos = self.active_segment.write(key_b, value_b)
        self.index[key_b] = (os.path.basename(self.active_segment.filename), pos)

    def get(self, key: str):
        key_b = key.encode()
        if key_b not in self.index:
            return None
        segment_name, pos = self.index[key_b]
        segment = Segment(self.directory, int(segment_name.split('-')[1].split('.')[0]))
        _, value = segment.read_at(pos)
        segment.close()
        return value.decode()

    def delete(self, key: str):
        key_b = key.encode()
        if key_b in self.index:
            self.active_segment.write(key_b, TOMBSTONE)
            del self.index[key_b]

    def merge_segments(self):
        merged = {}
        segments = sorted([f for f in os.listdir(self.directory) if f.startswith('segment-')])
        for segment in segments:
            path = os.path.join(self.directory, segment)
            with open(path, 'rb') as f:
                while header := f.read(8):
                    key_len, val_len = struct.unpack('>II', header)
                    key = f.read(key_len)
                    value = f.read(val_len)
                    if value != TOMBSTONE:
                        merged[key] = value
                    elif key in merged:
                        del merged[key]

        # Write to new segment
        self.active_segment.close()
        self.segment_id += 1
        merged_segment = Segment(self.directory, self.segment_id)
        self.index.clear()

        for key, value in merged.items():
            pos = merged_segment.write(key, value)
            self.index[key] = (os.path.basename(merged_segment.filename), pos)

        # Cleanup old segments
        for segment in segments:
            os.remove(os.path.join(self.directory, segment))

        self.active_segment = merged_segment

    def close(self):
        self.active_segment.close()


# Driver Code
db = Bitcask()
db.put("user2028","value2028")
db.put("user2029","value2020")
print(db.get("user2028"))
db.delete("user2028")
db.merge_segments()
print(db.get("user2028"))


