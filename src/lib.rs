use pyo3::prelude::*;

use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet};

/// A scheduler that handles the scheduling of a propagation procedure.
///
/// Internally, it stores a sorted map from time to a list of coordinate arrays.
/// Each coordinate array is represented as a Vec of rows, with each row being an array of 3 f64 values.

#[pyclass(module = "propagator_scheduler_rs", name = "Scheduler")]
pub struct Scheduler {
    list: BTreeMap<OrderedFloat<f64>, Vec<Vec<[usize; 3]>>>,
}

#[pymethods]
impl Scheduler {
    #[new]
    /// Creates a new, empty Scheduler.
    pub fn new() -> Self {
        Scheduler {
            list: BTreeMap::new(),
        }
    }

    /// Pushes an update into the scheduler.
    ///
    /// If the time key does not exist, a new entry is created.
    pub fn push(&mut self, coords: Vec<[usize; 3]>, time: f64) {
        let time_key = OrderedFloat(time);
        self.list
            .entry(time_key)
            .or_insert_with(Vec::new)
            .push(coords);
    }

    /// Pushes multiple updates into the scheduler.
    ///
    /// Each update is a tuple where the first element is the time and the second is the coordinate array.
    pub fn push_all(&mut self, updates: Vec<(f64, Vec<[usize; 3]>)>) {
        for (time, coords) in updates {
            self.push(coords, time);
        }
    }

    /// Pops the update with the smallest time value from the scheduler.
    ///
    /// Returns a tuple containing the time and the corresponding list of coordinate arrays.
    pub fn pop(&mut self) -> Option<(f64, Vec<Vec<[usize; 3]>>)> {
        // Since BTreeMap is sorted, the first key is the smallest.
        if let Some((&time, _)) = self.list.iter().next() {
            let updates = self.list.remove(&time).unwrap();
            Some((time.into_inner(), updates))
        } else {
            None
        }
    }

    /// Returns all the unique "active" thread identifiers.
    ///
    /// This iterates through all coordinate arrays, extracts the third element (index 2) from every row,
    /// and returns them in sorted order.
    pub fn active(&self) -> Vec<usize> {
        let mut unique_values = BTreeSet::new();
        for (_time, coords_list) in &self.list {
            for coords in coords_list {
                for row in coords {
                    unique_values.insert(row[2]);
                }
            }
        }
        unique_values.into_iter().collect()
    }

    /// Returns the number of scheduled time keys.
    pub fn len(&self) -> usize {
        self.list.len()
    }
}

// /// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }

/// A Python module implemented in Rust.
#[pymodule]
fn propagator_scheduler_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<Scheduler>()?;
    Ok(())
}
