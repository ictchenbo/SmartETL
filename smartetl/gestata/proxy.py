from random import randint


def select(values: list):
    return values[randint(0, len(values))]
