import pytest
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder
)


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4
])
def test_read_write_serde_v2(compression_type):
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=1,
        producer_id=123456, producer_epoch=123, base_sequence=9999,
        batch_size=10100010)
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super",
            headers=headers)
    buffer = builder.build()

    reader = DefaultRecordBatch(buffer.getvalue())
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == 9999999
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.headers == headers
