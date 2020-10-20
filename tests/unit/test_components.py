import pypes


def test_callable_proc_fn_should_run_func():
    # if we pass a function to callable proc fn
    # it should execute when we call the process
    # method
    def get_hello():
        yield "hello world"

    procfn = pypes.CallableProcFn(get_hello)
    generator = procfn.process()
    out = next(generator)

    assert out == "hello world"


def test_callable_proc_fn_should_run_lambda():
    # if we pass a lambda to callable proc fn
    # it should execute when we call the process
    # method
    procfn = pypes.CallableProcFn(lambda x: (yield f"the input was {x}"))
    generator = procfn.process("hello")
    out = next(generator)

    assert out == "the input was hello"


def test_relationship_should_iterate_destinations():
    # iterating a relationship should loop through
    # its destinations
    rel = pypes.Relationship()
    rel.destinations.append("hello")
    rel.destinations.append("goodbye")

    outs = []
    for dest in rel:
        outs.append(dest)

    assert outs == rel.destinations


def test_relationship_or_operator_should_return_other():
    # when piping a relationship to another item, the other
    # item should be returned
    rel = pypes.Relationship()
    out = rel | "other"

    assert out == "other"


def test_relationship_or_operator_should_append_other_to_dest():
    # when piping a relationship to another item, it should be
    # appended to the relationship destinations
    rel = pypes.Relationship()
    rel | "other"

    assert "other" in rel.destinations


def test_funnel_or_operator_should_append_other_to_all_inputs():
    # when piping a funnel to another item, it should be appended
    # to the destinations of all relationships in the funnel
    in_rel_1 = pypes.Relationship()
    in_rel_2 = pypes.Relationship()
    in_rel_3 = pypes.Relationship()

    funnel = pypes.Funnel(in_rel_1, in_rel_2, in_rel_3)

    funnel | "other"

    assert (
        "other" in in_rel_1.destinations
        and "other" in in_rel_2.destinations
        and "other" in in_rel_3.destinations
    )


def test_funnel_or_operator_should_return_other():
    # when piping a funnel to another item, the other item
    # should be returned
    in_rel_1 = pypes.Relationship()
    in_rel_2 = pypes.Relationship()
    in_rel_3 = pypes.Relationship()

    funnel = pypes.Funnel(in_rel_1, in_rel_2, in_rel_3)

    out = funnel | "other"

    assert out == "other"

def test_processor_should_run_lambda():
    # a processor should execute a lambda passed to the
    # constructor when the process method is called
    with pypes.Processor(lambda:(yield 'hello world')) as p:
        generator = p.process()
        out = next(generator)

        assert out == 'hello world'

def test_processor_should_run_function():
    # a processor should execute a function passed to the
    # constructor when the process method is called
    def get_message(data):
        yield f'the input was {data}'

    with pypes.Processor(get_message) as p:
        generator = p.process('hello')
        out = next(generator)

        assert out == 'the input was hello'

def test_processor_should_run_proc_fn():
    # a processor should execute a function passed to the
    # constructor when the process method is called
    class MessageFn(pypes.ProcFn):
        def process(self, data):
            yield f'the input was {data}'

    with pypes.Processor(MessageFn()) as p:
        generator = p.process('hello')
        out = next(generator)

        assert out == 'the input was hello'

def test_proccessor_or_operator_should_append_other_to_success_relationship():
    # when piping two processors, the destination processor should be appended
    # to the success relationship of the source processor
    src = pypes.Processor(lambda: (yield 'hello'))
    dest = pypes.Processor(lambda: (yield 'goodbye'))

    src | dest

    assert dest in src.relationships['success'].destinations

def test_processor_or_operator_should_return_other():
    # when piping two processors, the destination processor should be
    # returned
    src = pypes.Processor(lambda: (yield 'hello'))
    dest = pypes.Processor(lambda: (yield 'goodbye'))

    out = src | dest
    assert out == dest

def test_processor_rshift_should_return_named_relationship():
    # when using the rshift operator with a processor, a relationship
    # with the provided name should be returned
    proc = pypes.Processor(lambda: (yield 'hello'))
    failed = proc >> 'failed'

    assert proc.relationships['failed'] == failed

def test_runner_should_run_all_piped_processors():
    # the simple runner should execute all processors that
    # have been piped together
    exp_outs = ['foofoo', 'barbar', 'bazbaz']
    outs = []
    src = pypes.Processor(lambda: iter([('success', x) for x in ['foo', 'bar', 'baz']]))
    trn = pypes.Processor(lambda x: (yield 'success', x + x))
    dest = pypes.Processor(lambda x: outs.append(x))

    src | trn | dest

    runner = pypes.SimpleRunner()
    runner.run([src])

    assert outs == exp_outs

def test_pipeline_should_run_all_piped_processors():
    # the pipeline should execute all processors piped into the pipeline
    exp_outs = ['foofoo', 'barbar', 'bazbaz']
    outs = []

    with pypes.Pipeline() as p:
        (p
            | pypes.Processor(lambda: iter([('success', x) for x in ['foo', 'bar', 'baz']]))
            | pypes.Processor(lambda x: (yield 'success', x + x))
            | pypes.Processor(lambda x: outs.append(x))
        )

    assert outs == exp_outs
