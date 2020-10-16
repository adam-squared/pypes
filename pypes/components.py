"""Components for creating lightweight, stateless etl pipelines."""

import abc
import uuid
from typing import Iterator
from abc import abstractmethod


class Processor:
    """A Wrapper for a function, lambda or ProcFn object that allows
    processing logic to be chained together as part of a pipeline. Can
    also be used to perform standalone processing with a context manager.

    Args:
        fn (callable, ProcFn): a function, lambda or ProcFn that performs some
            processing operation on an optional input and returns an output.
        name (str): an optional name for the processor. If not specified, a
            uuid will be used.
    """

    def __init__(self, fn, name=None):
        self._relationships = {}
        self._id = str(uuid.uuid4())

        if name:
            self._name = name
        else:
            self._name = self._id

        if callable(fn):
            fn = CallableProcFn(fn)

        self._fn = fn

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self):
        self.teardown()

    def __or__(self, other):
        # pipe processors together using an implicit 'success' relationship
        rel = self._relationships.get("success")
        if not rel:
            rel = self._relationships["success"] = Relationship()
        return rel | other

    def __rshift__(self, other):
        # explicitly declare the output relationship to use for piping
        # processors together
        rel = self._relationships.get(other)

        if not rel:
            rel = self._relationships[other] = Relationship()

        return rel

    def setup(self):
        self._fn.setup()

    def teardown(self):
        self._fn.teardown()

    def process(self, *args):
        """Perform processing on the provided args (if any) and return any
        output.

        Returns:
            generator: a generator for iterating any output, or None, if there
                is no output (e.g a data sink).
        """
        # must return a generator, for now...
        return self._fn.process(*args)

    @property
    def relationships(self):
        return self._relationships

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id


class Relationship:
    """Represents a link between a source processor and zero or more
    destination processors. If a relationship doesn't have any destinations,
    any data sent to the relationship is discarded.
    """

    def __init__(self):
        self._destinations = []

    def __iter__(self):
        return iter(self._destinations)

    def __or__(self, other):
        self._destinations.append(other)
        return other

    @property
    def destinations(self):
        return self._destinations


class Funnel:
    """Connects multiple input relationships to a single destination
    processor, in effect flattening multiple data streams into a single
    stream. Processors or Relationships can be provided as args. If a
    Processor is provided, it will be linked to the destination via an
    implicit 'success' relationship.

    Args:
        args* (Processor or Relationship): A list of Processors and/or
            Relationships to connect to a destination processor.
    """

    def __init__(self, *args):
        self._inputs = args

    def __or__(self, other):
        for input in self._inputs:
            input | other
        return other


class ProcFn(abc.ABC):
    """An interface for creating custom processing tasks to pass to be run by
    a Processor. Provides optional setup and teardown methods (e.g for
    establishing/closing network/database connections) and a mandatory process
    method for defining a data processing task that operates on zero or one
    input.
    """

    def setup(self):
        pass

    def teardown(self):
        pass

    @abstractmethod
    def process(self, *args) -> Iterator[tuple]:
        """Performs a data processing operation on optional input args. Must return
        an iterator yielding two-tuples, where the output relationship name is the
        first element and the output data object is the second element.

        Args:
            *args (any): Optional input for processing
        """
        pass


class CallableProcFn(ProcFn):
    """Wraps a callable, such as a function or lambda so that it can be used
    as a pipeline step or standalone Processor.

    Args:
        fn (callable): A function or lambda to be used as for processing.
    """

    def __init__(self, fn):
        self.fn = fn

    def process(self, *args) -> Iterator[tuple]:
        return self.fn(*args)


class Runner(abc.ABC):
    @abstractmethod
    def run(self, procs, *args):
        """Run the provided Processors using the provided args as input.

        Args:
            procs (list): A list of Processors to execute.
            *args (any): optional input data for the Processors.
        """
        pass

class SimpleRunner:
    """Visits each Processor in a pipeline, retrieves its generator and then
    iterates the generator until it is exhausted, recursively passing each
    item of output to any destination processors.
    """

    def run(self, procs, *args):
        gens = {}
        for proc in procs:
            gen = proc.process(*args)
            if gen:
                gens[gen] = proc

        while gens:
            gens = self._process_next(gens)

    def _process_next(self, gens):
        # get the next item from each generator and recursively
        # execute all downstream processors with the ouput.
        removed = []
        for gen in gens.keys():
            out = None
            try:
                out = next(gen)
            except StopIteration:
                removed.append(gen)

            # if there is any output from the generator, run
            # the processors in the output relationship
            if out:
                rel = gens[gen].relationships.get(out[0])
                if rel:
                    self.run(rel.destinations, out[1])

        # get rid of any exhausted generators
        return {k: v for k, v in gens.items() if k not in removed}


class Pipeline:
    """Assembles and executes a pipeline. Once assembled, the pipeline is
    passed to the provided runner and executed.

    Pipelines support reading from multiple sources which can be of finite or
    infinite size. Processors can output to one or more relationships, that
    can each have zero or more destination processors. If a relationship has
    zero destinations, then any output sent to that relationship will be
    discarded. Multiple Processors can also be funnel output into a single
    processor.

    Args:
        runner (Runner): A runner for executing a pipeline. If no runner is
            provided a SimpleRunner will be used. Depending on your use case
            a different runner may provide better performance.
    """

    def __init__(self, runner=SimpleRunner()):
        self._sources = []
        self._runner = runner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.setup()
        self._runner.run(self._sources)
        self.teardown()

    def __or__(self, other):
        self._sources.append(other)
        return other

    def _get_graph(self):
        graph = {}

        def add(proc):
            node = graph.get(proc.id)
            if not node:
                node = graph[proc.id] = {}
                node["proc"] = proc
                for k, v in proc.relationships.items():
                    node[k] = [dest for dest in v]
                    for dest in v:
                        add(dest)

        for src in self._sources:
            add(src)

        return graph

    def setup(self):
        """Initialise all pipeline processors."""
        self._graph = self._get_graph()

        for node in self._graph.values():
            node["proc"].setup()

    def teardown(self):
        """Clean up any processor resources before terminating the
        pipeline.
        """
        for node in self._graph.values():
            node["proc"].teardown()
