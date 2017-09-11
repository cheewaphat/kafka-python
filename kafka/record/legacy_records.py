import io
import logging
import struct

from .abc import ABCRecord, ABCRecordBatch, ABCRecordBatchBuilder

from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode, lz4_encode_old_kafka
)
from kafka.protocol.message import MessageSet, Message
from kafka.protocol.types import Int64, Int32

log = logging.getLogger(__name__)


class LegacyRecordBatch(ABCRecordBatch):

    def __init__(self, buffer):
        bytes_to_read = len(buffer)
        buffer = io.BytesIO(buffer)
        messages = MessageSet.decode(buffer, bytes_to_read=bytes_to_read)
        assert len(messages) == 1
        self._message = messages[0]

    def validate_crc(self):
        return self._message[2].validate_crc()

    def __iter__(self):
        offset, size, msg = self._message
        if msg.is_compressed():
            # If relative offset is used, we need to decompress the entire
            # message first to compute the absolute offset.
            inner_mset = msg.decompress()
            if msg.magic > 0:
                last_offset, _, _ = inner_mset[-1]
                absolute_base_offset = offset - last_offset
            else:
                absolute_base_offset = -1

            for inner_offset, inner_size, inner_msg in inner_mset:
                # There should only ever be a single layer of compression
                assert not inner_msg.is_compressed(), (
                    'MessageSet at offset %d appears double-compressed. This '
                    'should not happen -- check your producers!' % offset)

                if msg.magic > 0:
                    # When magic value is greater than 0, the timestamp
                    # of a compressed message depends on the
                    # typestamp type of the wrapper message:
                    if msg.timestamp_type == 0:  # CREATE_TIME (0)
                        inner_timestamp = inner_msg.timestamp
                    else:  # LOG_APPEND_TIME (1)
                        inner_timestamp = msg.timestamp
                else:
                    inner_timestamp = None

                if absolute_base_offset >= 0:
                    inner_offset += absolute_base_offset

                yield LegacyRecord(
                    inner_offset, inner_timestamp, msg.timestamp_type,
                    inner_msg.key, inner_msg.value, inner_msg.crc)
        else:
            yield LegacyRecord(offset, msg.timestamp, msg.timestamp_type,
                               msg.key, msg.value, msg.crc)


class LegacyRecord(ABCRecord):

    __slots__ = ("_offset", "_timestamp", "_timestamp_type", "_key", "_value",
                 "_crc")

    def __init__(self, offset, timestamp, timestamp_type, key, value, crc):
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._key = key
        self._value = value
        self._crc = crc

    @property
    def offset(self):
        return self._offset

    @property
    def timestamp(self):
        """ Epoch milliseconds
        """
        return self._timestamp

    @property
    def timestamp_type(self):
        """ CREATE_TIME(0) or APPEND_TIME(1)
        """
        return self._timestamp_type

    @property
    def key(self):
        """ Bytes key or None
        """
        return self._key

    @property
    def value(self):
        """ Bytes value or None
        """
        return self._value

    @property
    def headers(self):
        return []

    @property
    def checksum(self):
        return self._crc

    def __repr__(self):
        return (
            "LegacyRecord(offset={!r}, timestamp={!r}, timestamp_type={!r},"
            " key={!r}, value={!r}, crc={!r})".format(
                self._offset, self._timestamp, self._timestamp_type,
                self._key, self._value, self._crc)
        )


class LegacyRecordBatchBuilder(ABCRecordBatchBuilder):

    LOG_OVERHEAD = struct.calcsize(
        ">q"  # Offset
        "i"   # Size
    )

    RECORD_OVERHEAD_V0 = struct.calcsize(
        ">i"  # CRC
        "b"   # magic
        "b"   # attributes
        "i"   # Key length
        "i"   # Value length
    )

    RECORD_OVERHEAD_V1 = struct.calcsize(
        ">i"  # CRC
        "b"   # magic
        "b"   # attributes
        "q"   # timestamp
        "i"   # Key length
        "i"   # Value length
    )

    def __init__(self, magic, compression_type, buffer=None):
        self._magic = magic
        self._compression_type = compression_type
        self._buffer = buffer if buffer is not None else io.BytesIO()

    def append(self, offset, timestamp, key, value, headers=None):
        """ Append message to batch.
        """
        assert not headers, "Headers not supported in v0/v1"

        if self._magic == 0:
            msg_inst = Message(value, key=key, magic=self._magic)
        else:
            msg_inst = Message(value, key=key, magic=self._magic,
                               timestamp=timestamp)

        crc = msg_inst.crc
        encoded = msg_inst.encode()
        msg = Int64.encode(offset) + Int32.encode(len(encoded)) + encoded
        self._buffer.write(msg)
        return crc, len(msg)

    def _maybe_compress(self):
        if self._compression_type:
            self._buffer.seek(0)
            data = self._buffer.read()
            if self._compression_type == Message.CODEC_GZIP:
                compressed = gzip_encode(data)
            elif self._compression_type == Message.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == Message.CODEC_LZ4:
                if self._magic == 0:
                    compressed = lz4_encode_old_kafka(data)
                else:
                    compressed = lz4_encode(data)

            msg = Message(compressed, attributes=self._compression_type,
                          magic=self._magic)
            encoded = msg.encode()

            self._buffer.seek(0)
            self._buffer.write(Int64.encode(0))
            self._buffer.write(Int32.encode(len(encoded)))
            self._buffer.write(encoded)
            self._buffer.truncate()
            return True
        return False

    def build(self):
        """Compress batch to be ready for send"""
        self._maybe_compress()
        return self._buffer

    def size_in_bytes(self, offset, timestamp, key, value, headers=None):
        """ Actual size of message to add
        """
        assert not headers, "Headers not supported in v0/v1"
        magic = self._magic
        return self.LOG_OVERHEAD + self.record_size(magic, key, value)

    @classmethod
    def record_size(cls, magic, key, value):
        message_size = cls.record_overhead(magic)
        if key is not None:
            message_size += len(key)
        if value is not None:
            message_size += len(value)
        return message_size

    @classmethod
    def record_overhead(cls, magic):
        assert magic in [0, 1], "Not supported magic"
        if magic == 0:
            return cls.RECORD_OVERHEAD_V0
        else:
            return cls.RECORD_OVERHEAD_V1

    @classmethod
    def estimate_size_in_bytes(cls, magic, compression_type, key, value):
        """ Upper bound estimate of record size.
        """
        assert magic in [0, 1], "Not supported magic"
        # In case of compression we may need another overhead for inner msg
        if compression_type:
            return (
                cls.LOG_OVERHEAD + cls.record_overhead(magic) +
                cls.record_size(magic, key, value)
            )
        return cls.LOG_OVERHEAD + cls.record_size(magic, key, value)
