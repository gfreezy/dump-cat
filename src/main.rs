#[macro_use]
extern crate structopt;

use std::path::PathBuf;

use failure::Fallible;
use structopt::StructOpt;

use message_tree_dumper::MessageTreeDumper;

mod message_tree;
mod message_tree_dumper;

#[derive(Debug, StructOpt)]
#[structopt(name = "dump-cat", about = "Dump cat logviews.")]
struct Opt {
    /// Activate debug mode
    #[structopt(short = "n", long = "number")]
    num: Option<usize>,
    /// Input file
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

fn main() -> Fallible<()> {
    let opt: Opt = Opt::from_args();
    let dumper = MessageTreeDumper::open(opt.path)?;
    let iter = dumper.into_iter();
    let count = if let Some(num) = opt.num {
        iter.take(num).count()
    } else {
        iter.count()
    };
    println!("count: {}", count);
    Ok(())
}
