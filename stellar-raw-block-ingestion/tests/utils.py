import random
import string

def generate_random_hash(length: int = 64) -> str:
    chars = string.hexdigits[:-6]
    hash_string = ''.join(random.choice(chars) for _ in range(length))
    return hash_string
