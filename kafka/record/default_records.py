# See:
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecordBatch.java
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecord.java

# RecordBatch and Record implementation for magic 2 and above.
# The schema is given below:

# RecordBatch =>
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  CRC => Uint32
#  Attributes => Int16
#  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
#  FirstTimestamp => Int64
#  MaxTimestamp => Int64
#  ProducerId => Int64
#  ProducerEpoch => Int16
#  BaseSequence => Int32
#  Records => [Record]

# Record =>
#   Length => Varint
#   Attributes => Int8
#   TimestampDelta => Varlong
#   OffsetDelta => Varint
#   Key => Bytes
#   Value => Bytes
#   Headers => [HeaderKey HeaderValue]
#     HeaderKey => String
#     HeaderValue => Bytes

# Note that when compression is enabled (see attributes below), the compressed
# record data is serialized directly following the count of the number of
# records. (ie Records => [Record], but without length bytes)

# The CRC covers the data from the attributes to the end of the batch (i.e. all
# the bytes that follow the CRC). It is located after the magic byte, which
# means that clients must parse the magic byte before deciding how to interpret
# the bytes between the batch length and the magic byte. The partition leader
# epoch field is not included in the CRC computation to avoid the need to
# recompute the CRC when this field is assigned for every batch that is
# received by the broker. The CRC-32C (Castagnoli) polynomial is used for the
# computation.

# The current RecordBatch attributes are given below:
#
# * Unused (6-15)
# * Control (5)
# * Transactional (4)
# * Timestamp Type (3)
# * Compression Type (0-2)

import struct
from .abc import ABCRecord, ABCRecordBatch
from .util import decode_varint, calc_crc32c

from kafka.errors import CorruptRecordException
from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode,
    gzip_decode, snappy_decode, lz4_decode
)


class DefaultRecordBatch(ABCRecordBatch):

    HEADER_STRUCT = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "i"  # PartitionLeaderEpoch => Int32
        "b"  # Magic => Int8
        "I"  # CRC => Uint32
        "h"  # Attributes => Int16
        "i"  # LastOffsetDelta => Int32 // also serves as LastSequenceDelta
        "q"  # FirstTimestamp => Int64
        "q"  # MaxTimestamp => Int64
        "q"  # ProducerId => Int64
        "h"  # ProducerEpoch => Int16
        "i"  # BaseSequence => Int32
        "i"  # Records count => Int32
    )
    # Byte offset in HEADER_STRUCT of attributes field. Used to calculate CRC
    ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
    CRC_OFFSET = struct.calcsize(">qiib")
    AFTER_LEN_OFFSET = struct.calcsize(">qi")

    CODEC_MASK = 0x07
    CODEC_NONE = 0x00
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08
    TRANSACTIONAL_MASK = 0x10
    CONTROL_MASK = 0x20

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0

    def __init__(self, buffer):
        self._buffer = memoryview(buffer)
        self._header_data = self.HEADER_STRUCT.unpack_from(self._buffer)
        self._pos = self.HEADER_STRUCT.size
        self._num_records = self._header_data[12]
        self._next_record_index = 0
        self._decompressed = False

    @property
    def base_offset(self):
        return self._header_data[0]

    @property
    def magic(self):
        return self._header_data[3]

    @property
    def crc(self):
        return self._header_data[4]

    @property
    def attributes(self):
        return self._header_data[5]

    @property
    def compression_type(self):
        return self.attributes & self.CODEC_MASK

    @property
    def timestamp_type(self):
        return int(bool(self.attributes & self.TIMESTAMP_TYPE_MASK))

    @property
    def is_transactional(self):
        return bool(self.attributes & self.TRANSACTIONAL_MASK)

    @property
    def is_control_batch(self):
        return bool(self.attributes & self.CONTROL_MASK)

    @property
    def first_timestamp(self):
        return self._header_data[7]

    @property
    def max_timestamp(self):
        return self._header_data[8]

    def _maybe_uncompress(self):
        if not self._decompressed:
            compression_type = self.compression_type
            if compression_type != self.CODEC_NONE:
                data = self._buffer[self._pos:]
                if compression_type == self.CODEC_GZIP:
                    uncompressed = gzip_decode(data)
                if compression_type == self.CODEC_SNAPPY:
                    uncompressed = snappy_decode(data.tobytes())
                if compression_type == self.CODEC_LZ4:
                    uncompressed = lz4_decode(data.tobytes())
                self._buffer = memoryview(uncompressed)
                self._pos = 0
        self._decompressed = True

    def _read_msg(self):
        # Record =>
        #   Length => Varint
        #   Attributes => Int8
        #   TimestampDelta => Varlong
        #   OffsetDelta => Varint
        #   Key => Bytes
        #   Value => Bytes
        #   Headers => [HeaderKey HeaderValue]
        #     HeaderKey => String
        #     HeaderValue => Bytes

        buffer = self._buffer
        pos = self._pos
        length, pos = decode_varint(buffer, pos)
        start_pos = pos
        _, pos = decode_varint(buffer, pos)  # attrs can be skipped for now

        ts_delta, pos = decode_varint(buffer, pos)
        if self.timestamp_type == self.LOG_APPEND_TIME:
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta

        offset_delta, pos = decode_varint(buffer, pos)
        offset = self.base_offset + offset_delta

        key_len, pos = decode_varint(buffer, pos)
        if key_len >= 0:
            key = buffer[pos: pos + key_len].tobytes()
            pos += key_len
        else:
            key = None

        value_len, pos = decode_varint(buffer, pos)
        if value_len >= 0:
            value = buffer[pos: pos + value_len].tobytes()
            pos += value_len
        else:
            value = None

        header_count, pos = decode_varint(buffer, pos)
        if header_count < 0:
            raise CorruptRecordException("Found invalid number of record "
                                         "headers {}".format(header_count))
        headers = []
        for _ in range(header_count):
            # Header key is of type String, that can't be None
            h_key_len, pos = decode_varint(buffer, pos)
            if h_key_len < 0:
                raise CorruptRecordException(
                    "Invalid negative header key size {}".format(h_key_len))
            h_key = buffer[pos: pos + h_key_len].tobytes().decode("utf-8")

            # Value is of type NULLABLE_BYTES, so it can be None
            h_value_len, pos = decode_varint(buffer, pos)
            if h_value_len >= 0:
                h_value = buffer[pos: pos + h_value_len].tobytes()
            else:
                h_value = None

            headers.append((h_key, h_value))

        # validate whether we have read all header bytes in the current record
        if pos - start_pos != length:
            CorruptRecordException(
                "Invalid record size: expected to read {} bytes in record "
                "payload, but instead read {}".format(length, pos - start_pos))
        self._pos = pos

        return DefaultRecord(
            offset, timestamp, self.timestamp_type, key, value, headers)

    def __iter__(self):
        self._maybe_uncompress()
        return self

    def __next__(self):
        if self._next_record_index >= self._num_records:
            if self._pos != len(self._buffer):
                raise CorruptRecordException(
                    "{} unconsumed bytes after all records consumed".format(
                        len(self._buffer) - self._pos))
            raise StopIteration
        try:
            msg = self._read_msg()
        except ValueError as err:
            raise CorruptRecordException(
                "Found invalid record structure: {!r}".format(err))
        else:
            self._next_record_index += 1
        return msg

    next = __next__

    def validate_crc(self):
        assert self._decompressed is False, \
            "Validate should be called before iteration"

        crc = self.crc
        data_view = self._buffer[self.ATTRIBUTES_OFFSET:]
        verify_crc = calc_crc32c(data_view)
        return crc == verify_crc


class DefaultRecord(ABCRecord):

    __slots__ = ("_offset", "_timestamp", "_timestamp_type", "_key", "_value",
                 "_headers")

    def __init__(self, offset, timestamp, timestamp_type, key, value, headers):
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._key = key
        self._value = value
        self._headers = headers

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
        return self._headers

    @property
    def checksum(self):
        return None
