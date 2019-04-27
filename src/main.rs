extern crate structopt;

use std::convert::TryFrom;
use std::path::PathBuf;

use env_logger::Env;
use evalexpr::*;
use failure::Fallible;
use log::info;
use structopt::StructOpt;

use crate::message_tree_dumper::MessageTreeDumper;
use crossbeam::RecvTimeoutError;
use message_tree_dumper::MessageTreeDumperBuilder;
use std::thread;
use std::time::Duration;

mod message_tree;
mod message_tree_dumper;

#[derive(Debug, StructOpt)]
#[structopt(name = "dump-cat", about = "Dump cat logviews.")]
struct Opt {
    #[structopt(short = "n", long = "number")]
    num: Option<usize>,
    #[structopt(
        short = "q",
        long = "query",
        help = "variables: [status|ty|name|timestamp_in_ms|transaction.duration_in_ms]"
    )]
    query: Option<String>,
    #[structopt(long = "json", help = "output as json")]
    json: bool,
    #[structopt(long = "quiet", help = "for benchmark only")]
    quiet: bool,
    /// Input file
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(long = "decoding-threads", default_value = "1")]
    decoding_threads: usize,
    #[structopt(long = "filter-threads", default_value = "1")]
    filter_threads: usize,
    #[structopt(long = "block-reader-channel-buffer-size", default_value = "10")]
    block_reader_channel_buffer_size: usize,
    #[structopt(long = "tree-decoder-channel-buffer-size", default_value = "10")]
    tree_decoder_channel_buffer_size: usize,
}

fn main() -> Fallible<()> {
    env_logger::from_env(Env::default().default_filter_or("warn")).init();

    let opt: Opt = Opt::from_args();
    let dumper = MessageTreeDumperBuilder::default()
        .path(opt.path)
        .threads(opt.decoding_threads)
        .block_reader_channel_buffer_size(opt.block_reader_channel_buffer_size)
        .tree_decoder_channel_buffer_size(opt.tree_decoder_channel_buffer_size)
        .build();
    let dumper: MessageTreeDumper = match dumper {
        Ok(d) => d,
        Err(s) => panic!(s),
    };

    let mut count = opt.num.unwrap_or(usize::max_value());
    let show_json = opt.json;
    let quiet = opt.quiet;

    let recv = dumper.read_trees();
    let mut handles = vec![];
    for i in 0..opt.filter_threads {
        let recv = recv.clone();
        let query = opt.query.clone();

        let handle = thread::Builder::new()
            .name(format!("FilterThread{}", i))
            .spawn(move || -> Fallible<()> {
                let precompiled = query.map(|q| build_operator_tree(&q)).transpose()?;

                loop {
                    let tree = match recv.recv_timeout(Duration::from_millis(5)) {
                        Ok(t) => t,
                        Err(RecvTimeoutError::Timeout) => {
                            info!("Waiting for new MessageTree.");
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break;
                        }
                    };

                    let mut context = HashMapContext::new();
                    context.set_value("status".into(), tree.message.status().as_str().into())?;
                    context.set_value("ty".into(), tree.message.ty().as_str().into())?;
                    context.set_value("name".into(), tree.message.name().as_str().into())?;
                    context.set_value(
                        "timestamp_in_ms".into(),
                        i64::try_from(tree.message.ts())?.into(),
                    )?;
                    if let Some(duration) = tree.message.duration_in_ms() {
                        context.set_value(
                            "transaction.duration_in_ms".into(),
                            (duration as i64).into(),
                        )?;
                    }

                    let match_ret = if let Some(expr) = &precompiled {
                        expr.eval_boolean_with_context(&context)?
                    } else {
                        true
                    };

                    if match_ret {
                        if count > 0 {
                            if !quiet {
                                if show_json {
                                    println!("{}", serde_json::to_string(&tree.message)?);
                                } else {
                                    println!("{}", tree.message);
                                }
                            }
                            count -= 1;
                        } else {
                            break;
                        }
                    }
                }

                Ok(())
            })?;
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("join")?;
    }

    Ok(())
}
