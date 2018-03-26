#![feature(integer_atomics)]
#![feature(collections)]
#![feature(option_ref_mut_cloned)]
#![feature(fnbox)]
#![feature(conservative_impl_trait)]
#![feature(iterator_step_by)]
#![feature(get_type_id)]
#![feature(option_filter)]

pub extern crate volatile_cell;
pub extern crate monitor;
pub extern crate num_cpus;
pub extern crate rand;
pub extern crate time;
pub extern crate stopwatch;

pub mod threading;

fn main() {
    println!("Hello, world!");
}
