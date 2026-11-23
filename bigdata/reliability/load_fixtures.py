"""Separate burst workload: 20 themes, 20 active issue candidates per theme."""
import hashlib
import math
import random

THEMES = 20
CANDIDATES = 20
THEME_FIRST = 910000
ISSUE_FIRST = 1100000


def normalize(values):
    length = math.sqrt(sum(value * value for value in values))
    return [round(value / length, 8) for value in values]


def prototype(theme):
    return normalize([math.sin(index * 0.113 + theme * 0.27) + 0.1 for index in range(768)])


def issue_embedding(theme, candidate):
    center = prototype(theme)
    if candidate == 0:
        return center
    generator = random.Random(theme * 1000 + candidate)
    return normalize([value + generator.uniform(-0.012, 0.012) for value in center])


def article_embedding(article_id, theme):
    generator = random.Random(int(hashlib.sha256(article_id.encode()).hexdigest()[:16], 16))
    return normalize([value + generator.uniform(-0.002, 0.002) for value in prototype(theme)])


def expected_issue(index):
    return ISSUE_FIRST + index % THEMES * CANDIDATES
