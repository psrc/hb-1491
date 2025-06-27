import argparse
from pathlib import Path

def add_run_args(parser, multiprocess=True):
    """
    Run command args
    """
    parser.add_argument('-c', '--configs_dir',
                        type=Path,
                        metavar='PATH',
                        default=Path(__file__).absolute().parent,
                        help='path to configs dir')


# if __name__ == '__main__':
parser = argparse.ArgumentParser()
add_run_args(parser)
args = parser.parse_args()