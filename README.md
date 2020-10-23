# pypes
![build](https://github.com/adam-squared/pypes/workflows/build/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A framework for building lightweight, stateless data processing pipelines with python.

## Features

- Concise syntax for assembling and executing a data pipeline
- Consume data from multiple bound or unbound sources
- Easily create processing steps from functions, lambdas or the ProcFn class for more complex tasks
- Control flow by defining relationships between processors
- Route data to multiple destinations, or funnel multiple sources into a single destination

## Quickstart

1. Import some pipeline components

    ```python
    from pypes import Pipeline, Processor
    ```

2. Define your processing steps as functions, lambdas or ProcFns. You can accept zero or more args but you
must return an iterator of two-tuples, where the first element is the output relationship name and the second
is the output data.

    ```python
    def get_number_pairs():
        while True:
            a = random.randint(1, 100)
            b = random.randint(1, 100)
            yield "success", (a, b)

    def add_numbers(data):
        a = data[0]
        b = data[1]
        yield "success", a + b
    ```

3. Assemble a pipeline from your processing steps by piping them together and wrapping them in a Processor.

    ```python
    with Pipeline() as p:
        (p
            | Processor(get_number_pairs)
            | Processor(add_numbers)
            | Processor(lambda x: print(f'the result is {x}'))
        )
    ```

4. Run your script to execute the pipeline!

    ```bash
    python my_pipeline_script.py
    ```
