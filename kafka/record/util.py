
def make_slice(buf, slice_start, slice_end):
    """ No copy way to get buffer interface compatible with struct.unpack
    """
    return memoryview(buf)[slice_start:slice_end]
