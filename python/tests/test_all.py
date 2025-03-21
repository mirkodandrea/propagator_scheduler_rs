import pytest
from propagator_scheduler_rs import Scheduler

def test_scheduler_initialization():
    """Test that a Scheduler can be initialized."""
    scheduler = Scheduler()
    assert len(scheduler) == 0

def test_push_all():
    """Test pushing multiple updates at once."""
    scheduler = Scheduler()
    updates = [
        (3.0, [7, 8, 9]),
        (1.0, [1, 2, 3]),
        (2.0, [4, 5, 6]),
    ]
    scheduler.push_all(updates)

    assert len(scheduler) == 3

    times = []
    while len(scheduler) > 0:
        time, _ = scheduler.pop()
        times.append(time)

    assert times == [1.0, 2.0, 3.0]  # Ensuring order

def test_active():
    """Test retrieving active thread identifiers."""
    scheduler = Scheduler()
    scheduler.push_all([
        (1.0, [1, 2, 3]),
        (1.0, [4, 5, 6]),
        (2.0, [7, 8, 9]),
    ])
    

    active_threads = scheduler.active()
    assert set(active_threads) == {3, 6, 9}

def test_empty_pop():
    """Test that popping an empty scheduler returns None."""
    scheduler = Scheduler()
    assert scheduler.pop() is None

def test_million_adds():
    """ Test adding a million updates to the scheduler. """
    scheduler = Scheduler()
    from random import random
    
    for i in range(1000000):
        scheduler.push(i%1000, [int(random()) for _ in range(3)])
    
    
    assert len(scheduler) == 1000