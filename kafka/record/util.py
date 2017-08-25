import six


def make_slice(buf, slice_start, slice_end):
    """ No copy way to get buffer interface compatible with struct.unpack
    """
    if six.PY3:
        return memoryview(buf)[slice_start:slice_end]
    else:
        return buffer(buf, slice_start, slice_end - slice_start)
