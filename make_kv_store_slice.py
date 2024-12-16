
import argparse
from src.utils import BlockFolderL2, BlockDataFolderL1,KVDatabase, BlockChainParse
import os

LEVELDB_FILES_ROOT = 'leveldb_files'

parser = argparse.ArgumentParser(description="A sample program to demonstrate command-line flags")

# Add arguments
# parser.add_argument('-h', '--help', help="print this help and exit.")
parser.add_argument('--start', type=int, help='start of the slice to be processed')
parser.add_argument('--end', type=int, help='end of the slice to be processed')
parser.add_argument('--toplevel_folder_idx', type=int, default = 0, help='The top-level folder index. The folder/file iterator, sorts by starting block each folder (e.g., if the first avail folder is Block_663300_to_736900_xx, the index [0] will return files from that folder.)')

if __name__ == '__main__':    
    args = parser.parse_args()
    if any([v is None for v in [args.start, args.end]]):
        raise Exception("Start and end of the slice need to be provided! ")
    
    if not os.path.exists(LEVELDB_FILES_ROOT):
        os.makedirs(LEVELDB_FILES_ROOT)
    
    kv_db_file = os.path.join('.',LEVELDB_FILES_ROOT, f'kv_db_{args.start}_to_{args.end}.ldb')
    print(f" - writing new KV store in {kv_db_file}")
    b1 = BlockDataFolderL1('data')
    _it = b1[args.toplevel_folder_idx].slice_iterator( args.start, args.end)
    BlockChainParse(_it , kv_db_file).parse_merge()