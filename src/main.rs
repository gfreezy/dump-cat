extern crate structopt;

use std::convert::TryFrom;
use std::path::PathBuf;

use env_logger::Env;
use evalexpr::*;
use failure::Fallible;
use structopt::StructOpt;

use message_tree_dumper::MessageTreeDumper;

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
    /// Input file
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

fn main() -> Fallible<()> {
    env_logger::from_env(Env::default().default_filter_or("warn")).init();

    let opt: Opt = Opt::from_args();
    let dumper = MessageTreeDumper::open(opt.path)?;

    let query = opt.query;
    let precompiled = query.map(|q| build_operator_tree(&q)).transpose()?;

    let mut count = opt.num.unwrap_or(usize::max_value());

    for tree in dumper {
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
                if opt.json {
                    println!("{}", serde_json::to_string(&tree.message)?);
                } else {
                    println!("{}", tree.message);
                }
                count -= 1;
            } else {
                break;
            }
        }
    }

    Ok(())
}
