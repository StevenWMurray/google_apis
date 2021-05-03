import argparse
from typing import Tuple
from more_itertools import consumer
from datetime import date

@consumer
def get_dates(parser: argparse.ArgumentParser) -> Tuple[date, date]:
    """Grab the data pull date range bounds from the command line

    The relevant arguments:
    * -d / --date: Requires a single date
    * --dates: Requires 2 dates forming a start / end date pair
    
    These two specifications are mutually exclusive. Either a date should be
    provided, or a start date / end date pair.
    """
    dgroup = parser.add_mutually_exclusive_group(required=True)
    dgroup.add_argument(
        '-d', '--date',
        help='Pull data for a single date',
        type=date.fromisoformat)
    dgroup.add_argument(
        '--dates',
        nargs=2,
        metavar=('START_DATE', 'END_DATE'),
        help='Pull data for the bounded date range START_DATE END_DATE',
        type=date.fromisoformat)
    args = (yield)

    if args.date is not None:
        yield (args.date, args.date)
    yield tuple(args.dates)

def parse_args(*args, **kwargs) -> dict:
    """Flexible interface for a script to request one or more argument parsers.

    Parse args allows declaring both positional arguments and keyword-based
    option flags. Each positional argument, or value in a key / value pair,
    should be a function that:
    * Accepts an instance of argparse.ArgumentParser()
    * Adds one or more arguments to the parser before yielding control back
    * Recieves an args list passed via Generator.send()
    * Returns some parsed data based on the args list, depending only on the
        args it declared on the parser

    Note that some of these inputs may be used in either API calls or database
    queries. It's the responsibility of the parser function, not the end user,
    to implement appropriate input sanitation.

    Each option passed to parse_args should be a keyword / generator pair.
    The function returns a dict pairing the keywords with the values returned
    by their functions.
    """
    parser = argparse.ArgumentParser()
    argv = [arg(parser) for arg in args]
    opts = {key: kwargs[key](parser) for key in kwargs}

    parsed = parser.parse_args()
    argv = [arg.send(parsed) for arg in argv]
    opts = {key: opts[key].send(parsed) for key in opts}
    opts['args'] = argv
    return opts

if __name__ == "__main__":
    print(parse_args(dates=get_dates))
