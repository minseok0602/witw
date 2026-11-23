"""Deterministic, non-sparse 768-dimensional inference fixtures."""
import hashlib
import math
import random


def prototype():
    values = [math.sin(index * 0.113) + 0.1 for index in range(768)]
    norm = math.sqrt(sum(value * value for value in values))
    return [round(value / norm, 8) for value in values]


BASE = prototype()


def embedding(article_id):
    randomizer = random.Random(int(hashlib.sha256(article_id.encode()).hexdigest()[:16], 16))
    values = [value + randomizer.uniform(-0.002, 0.002) for value in BASE]
    norm = math.sqrt(sum(value * value for value in values))
    return [round(value / norm, 8) for value in values]
