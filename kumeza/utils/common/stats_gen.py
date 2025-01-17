import gc
import sys
from typing import Any


# Function to get the size of an object
def get_actual_size(input_objects: Any) -> float:
    mem_size = 0
    ids = set()
    objects = [input_objects]
    while objects:
        new = []
        for obj in objects:
            if id(obj) not in ids:  # get the object memory address
                ids.add(id(obj))  # and check for uniqueness
                mem_size += sys.getsizeof(obj)  # get size for each obj
                new.append(obj)
        objects = gc.get_referents(*new)
    return mem_size
