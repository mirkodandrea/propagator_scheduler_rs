import pytest
from propagator_scheduler_rs import Scheduler

def test_scheduler_initialization():
    """Test that a Scheduler can be initialized."""
    scheduler = Scheduler()
    assert scheduler.len() == 0

def test_push_single_update():
    """Test pushing a single update and retrieving it."""
    scheduler = Scheduler()
    scheduler.push([[1, 2, 3], [4, 5, 6]], 1.0)
    
    assert scheduler.len() == 1

    popped = scheduler.pop()
    assert popped is not None
    time, updates = popped
    assert time == 1.0
    assert updates == [[[1, 2, 3], [4, 5, 6]]]

def test_push_multiple_updates():
    """Test pushing multiple updates and popping them in order."""
    scheduler = Scheduler()
    scheduler.push([[1, 2, 3]], 2.0)
    scheduler.push([[4, 5, 6]], 1.0)

    assert scheduler.len() == 2

    popped1 = scheduler.pop()
    assert popped1 is not None
    time1, updates1 = popped1
    assert time1 == 1.0
    assert updates1 == [[[4, 5, 6]]]

    popped2 = scheduler.pop()
    assert popped2 is not None
    time2, updates2 = popped2
    assert time2 == 2.0
    assert updates2 == [[[1, 2, 3]]]

    assert scheduler.len() == 0

def test_push_all():
    """Test pushing multiple updates at once."""
    scheduler = Scheduler()
    updates = [
        (3.0, [[7, 8, 9]]),
        (1.0, [[1, 2, 3]]),
        (2.0, [[4, 5, 6]]),
    ]
    scheduler.push_all(updates)

    assert scheduler.len() == 3

    times = []
    while scheduler.len() > 0:
        time, _ = scheduler.pop()
        times.append(time)

    assert times == [1.0, 2.0, 3.0]  # Ensuring order

def test_active():
    """Test retrieving active thread identifiers."""
    scheduler = Scheduler()
    scheduler.push([[1, 2, 3], [4, 5, 6]], 1.0)
    scheduler.push([[7, 8, 9]], 2.0)

    active_threads = scheduler.active()
    assert set(active_threads) == {3, 6, 9}

def test_empty_pop():
    """Test that popping an empty scheduler returns None."""
    scheduler = Scheduler()
    assert scheduler.pop() is None
