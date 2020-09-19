import os
import random
import string


class Misc:
    @staticmethod
    def linspace(start, end, count):
        step = (end - start) / count
        assert end > start
        while start <= end:
            yield start
            start += step

    @staticmethod
    def get_random_string(length, only_lowercase: bool = False) -> str:
        if only_lowercase:
            character_choices = string.ascii_lowercase
        else:
            character_choices = string.ascii_letters
        return "".join(random.choices(character_choices, k=length))

    @staticmethod
    def create_directory_if_not_exists(directory: str) -> None:
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def chunkenize(iterator, chunk_size):
        current_chunk = []
        current_chunk_length = 0
        for element in iterator:
            current_chunk.append(element)
            current_chunk_length += 1
            if current_chunk_length == chunk_size:
                yield current_chunk
                current_chunk = []
                current_chunk_length = 0

        yield current_chunk
