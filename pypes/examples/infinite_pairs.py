import random
import string
import logging

from pypes import Processor, Funnel, Pipeline


def add(data):
    # add or append items in a two-tuple
    # route to failure if the op fails
    a = data[0]
    b = data[1]

    try:
        yield "success", a + b
    except TypeError:
        yield "failure", (a, b)


def minus(data):
    # minus items in a two-tuple
    # route to failure if the op fails
    a = data[0]
    b = data[1]
    try:
        yield "success", a - b
    except TypeError:
        yield "failure", (a, b)


def get_number_pairs():
    # infintely generate pairs of random numbers
    while True:
        a = random.randint(1, 100)
        b = random.randint(1, 100)
        yield "success", (a, b)


def get_word_pairs():
    # infinitely generate pairs of random words
    while True:
        a = "".join(random.choice(string.ascii_lowercase) for i in range(8))
        b = "".join(random.choice(string.ascii_lowercase) for i in range(8))
        yield "success", (a, b)


# create a pipeline with two unbounded sources. try to add and
# subtract the pairs of words and numbers. Print the result on
# success, or log an error on failure.
with Pipeline() as p:
    numbers = p | Processor(get_number_pairs)
    words = p | Processor(get_word_pairs)
    add_nums = Funnel(numbers, words) | Processor(add)
    minus_nums = Funnel(numbers, words) | Processor(minus)
    on_success = Funnel(add_nums, minus_nums) | Processor(
        lambda x: print(f"the result is {x}")
    )
    on_add_failure = add_nums >> "failure" | Processor(
        lambda x: logging.error(f"could not add {x[0]} and {x[1]}")
    )
    on_minus_failure = minus_nums >> "failure" | Processor(
        lambda x: logging.error(f"could not subtract {x[0]} and {x[1]}")
    )
