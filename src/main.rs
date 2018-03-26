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
pub extern crate serde;
pub extern crate tokio;
pub extern crate local_ip;
pub extern crate ssh2 as ssh;
pub extern crate file;

pub mod threading;

use std::process::exit;
use std::io;
use std::io::Write;
use std::path::Path;

fn prompt_line(prompt: &str) -> String {
    print!("{}:\n> ", prompt);
    io::stdout().flush().unwrap();
    let mut result = String::new();
    io::stdin().read_line(&mut result).expect("failed to read line");
    result
}

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    match args.get(1) {
        Some(s) if s == "maestro" => {
            println!("running master");

            let address_path = prompt_line("provide helper address file");
            let addresses_file = Path::new(address_path.trim());
            let addresses = match file::get_text(&addresses_file) {
                Ok(text) => text,
                Err(err) => {
                    eprintln!("failed to read text from file {:?}:", addresses_file);
                    eprintln!("{:?}", err);
                    exit(1);
                }
            };

            for address in addresses.trim().split("\n") {
                println!("address: {}", address);
            }
        },
        Some(s) if s == "helper" => {
            println!("running helper");
        },
        other => {
            eprintln!("usage: ./clusterzip <maestro/helper>");
            eprintln!("invalid arg: {:?}", other);
            exit(1);
        }
    };
}
