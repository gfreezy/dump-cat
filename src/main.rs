use failure::Fallible;

use message_tree_dumper::MessageTreeDumper;

mod message_tree;
mod message_tree_dumper;

fn main() -> Fallible<()> {
    let dumper = MessageTreeDumper::open("cat-dat/cat-10.9.185.69.dat")?;
    println!("count: {}", dumper.into_iter().take(1000).count());
    Ok(())
}
