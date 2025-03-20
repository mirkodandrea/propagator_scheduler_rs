use pyo3::prelude::*;
use rustc_hash::FxHashSet;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const PRECISION: f64 = 10.0;
/// A scheduler that handles the scheduling of a propagation procedure.
/// Uses a **BinaryHeap** to maintain order efficiently.
#[pyclass(module = "propagator_scheduler_rs", name = "Scheduler")]
pub struct Scheduler {
    heap: BinaryHeap<Reverse<(u32, Vec<[usize; 3]>)>>, // Min-heap of (time, updates)
}

#[pymethods]
impl Scheduler {
    #[new]
    pub fn new() -> Self {
        Scheduler {
            heap: BinaryHeap::new(),
        }
    }

    /// Pushes an update into the scheduler in **O(log n) time**.
    pub fn push(&mut self, coords: Vec<[usize; 3]>, time: f64) {
        let time_key = f64::round(time * PRECISION) as u32;
        self.heap.push(Reverse((time_key, coords)));
    }

    /// Pushes multiple updates in **O(m log n) time** (m = # of updates).
    pub fn push_all(&mut self, updates: Vec<(f64, Vec<[usize; 3]>)>) {
        for (time, coords) in updates {
            let time_key = f64::round(time * PRECISION) as u32;
            self.heap.push(Reverse((time_key, coords)));
        }
    }

    /// Pops the earliest event in **O(log n) time**.
    pub fn pop(&mut self) -> Option<(f64, Vec<[usize; 3]>)> {
        self.heap
            .pop()
            .map(|Reverse(event)| (f64::from(event.0) / PRECISION, event.1))
    }

    /// Returns unique "active" thread identifiers in **O(n) time** using a fast HashSet.
    pub fn active(&self) -> Vec<usize> {
        let mut unique_values: FxHashSet<usize> = FxHashSet::default();
        for Reverse((_time, coords_list)) in &self.heap {
            for row in coords_list {
                unique_values.insert(row[2]); // Extract the third element
            }
        }
        let mut result: Vec<usize> = unique_values.into_iter().collect();
        result.sort_unstable(); // Sorting is optional
        result
    }

    /// Returns the number of scheduled events in **O(1) time**.
    pub fn __len__(&self) -> usize {
        self.heap.len()
    }

    /// Checks if the scheduler is empty in **O(1) time**.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

/// Python module for Rust-based scheduler.
#[pymodule]
fn propagator_scheduler_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Scheduler>()?;
    Ok(())
}
