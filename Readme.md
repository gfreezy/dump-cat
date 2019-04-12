# Dump-cat
Dump-cat is a tool for dumping [cat](https://github.com/dianping/cat) logviews.

## Installation

```bash
git clone https://github.com/gfreezy/dump-cat.git
cd dump-cat
cargo build --release
```

## Usage

```
dump-cat 0.1.0
gfreezy <gfreezy@gmail.com>
Dump cat logviews.

USAGE:
    dump-cat [OPTIONS] <path>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -n, --number <num>     
    -q, --query <query>    variables: [status|ty|name|ts|transaction.duration_in_ms|transaction.duration_start]

ARGS:
    <path>    Input file

```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
