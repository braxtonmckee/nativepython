from typed_python import sha_hash


def index_value_to_hash(value, serializationContext=None):
    if isinstance(value, int):
        return b"int_" + str(value).encode("utf8")
    if isinstance(value, str) and len(value) < 37:
        return b"str_" + str(value).encode("utf8")
    return b"hash_" + sha_hash(value, serializationContext).digest
