# pypes
![build](https://github.com/adam-squared/pypes/workflows/build/badge.svg)

A framework for building lightweight, stateless data processing pipelines with python.

## Features

- Concise syntax for assembling and executing a data pipeline
- Consume data from multiple bound or unbound sources
- Easily create processing steps from functions, lambdas or the ProcFn class for more complex tasks
- Control flow by defining relationships between processors
- Route data to multiple destinations, or funnel multiple sources into a single destination

## Quick Start

1. Define your processing steps, as functions, lambdas or ProcFns:
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

2. Assemble a pipeline from your steps
    ```python
    with Pipeline() as p:
        (p
            | Processor(get_number_pairs)
            | Processor(add_numbers)
            | Processor(lambda x: print(f'the result is {x}'))
        )
    ```

3. Run your pipeline script and crunch your data
    ```bash
    python my_pipeline_script.py
    ```
