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
    in_rel_1 = pypes.Relationship()
    in_rel_2 = pypes.Relationship()
    in_rel_3 = pypes.Relationship()

    funnel = pypes.Funnel(in_rel_1, in_rel_2, in_rel_3)

    out = funnel | "other"

    assert out == "other"
