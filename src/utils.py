import pandas as pd
import os
import plyvel
import pickle
from tqdm import tqdm

def _parse_utx(dat, io = 'inputs'):
    d = {'block_height' : int(dat[0]), 'transaction_hash' : dat[1],io : {}}

    if len(dat[2:]) == 1 and int(dat[2]) == 1:
        return d
    
    _idx = 2
    _dat_d = {}

    for i in range(int(dat[2])):
        try:
            utx_i = dat[_idx+1]
            utx_v = dat[_idx+2]
            _idx+=2
            _dat_d[utx_i] = float(utx_v)
        except:
            print("------")
            print(dat)
            print("Problem while processing the above data!")
            print("issues will be ignored.")
    d.update({io : _dat_d})
    return d

def _parse_blocks_csv(_fpath, io = 'inputs'):
    dd = pd.read_csv(_fpath,header = None)
    res = []
    for kkk, _d in enumerate(dd.values):
        try:
            dat = _parse_utx(_d[0].split('\t'), io = io)
        except:
            print(f"exception encountered when parsing the data - line: {kkk}")

        res.append(dat)
    return res

_get_block_num = lambda s : int(s.split('/')[-1].split('_')[0])

class BlockFolderL2:
    def __init__(self, fpath_main = None):
        self.fpath_main = fpath_main
        self.fpath_in = fpath_main +'_IN'
        print(self.fpath_in)
        # self.fpath_out = fpath_main.replace("_IN",'_OUT').replace('_in','_out')
        self.in_csv_file_paths = [os.path.join(self.fpath_in, ff) for ff in  os.listdir(self.fpath_in)]
        self.out_csv_file_paths = [f.replace('_IN','_OUT').replace('_in','_out') for f in self.in_csv_file_paths]
        self.in_csv_file_paths = sorted(self.in_csv_file_paths, key = _get_block_num)
        self.out_csv_file_paths = sorted(self.out_csv_file_paths, key = _get_block_num)

    def __len__(self):
        return len(self.in_csv_file_paths)
    
    def slice_iterator(self, slice_start, slice_end):
        for ii in range(slice_start, slice_end):
            yield self[ii]
    def __getitem__(self,k):
        _fpath_in = self.in_csv_file_paths[k]
        _fpath_out = self.out_csv_file_paths[k]
        res_inputs = _parse_blocks_csv(_fpath_in,'inputs')
        res_outputs = _parse_blocks_csv(_fpath_out, 'outputs')
        return res_inputs, res_outputs
    
class BlockDataFolderL1:
    def __init__(self, main_folder = 'data'):
        self.main_folder = main_folder
        self.main_paths_all = [d[:-3] for d in os.listdir(main_folder) if 'rar' not in d and 'IN' in d]
        self.main_paths_all = sorted(self.main_paths_all, key = lambda x : int(x.split('_')[1]))
    def __len__(self):
        return len(self.main_paths_all)
    def __getitem__(self, k):
        fpath_main = self.main_paths_all[k]
        return BlockFolderL2(os.path.join(self.main_folder, fpath_main))

class Pickler:
    def enc(self, dat):
        return pickle.dumps(dat)
    def dec(self, dat):
        return pickle.loads(dat)
    
class KVDatabase:
    """A KV database (with buffering/disk offloading etc) using
    levelDB in the bakckground
    """
    def __init__(self, fpath, create_if_missing = True, encdec = None):
        self.create_if_missing = create_if_missing
        self.fpath = fpath
        self.encdec = encdec 
        if encdec is None:
            self.encdec = Pickler()
        self.put_counter = 0
        
    def db(self):
        return plyvel.DB(self.fpath, create_if_missing = self.create_if_missing)
    def put_dict(self, d : dict, encode = True):
        def _maybe_enc(x):
            if encode:
                return self.encdec.enc(x)
            return x                    
        with self.db() as db:
            for k, v in d.items():
                k, v = _maybe_enc(k), _maybe_enc(v)
                db.put(k, v)
        self.put_counter += len(d)
    def __len__(self):
        return self.put_counter
    def get_many(self, keys, encode_keys = True, decode_outputs = True):
        """Gets a list of keys, and returns a corresponding dictionary, with the keys filled from 
        the Key-value store. 

        Args:
            keys           : list of keys 
            encode_keys    : (bool : True)  whether to encode the keys (e.g., in binary format) before the query 
            decode_outputs : (bool : True)  whether to decode (e.g., from pickle) the elements before returning.
        """
        def _maybe_encode_keys(x):
            if encode_keys:
                return self.encdec.enc(x)
            return x

        def _maybe_decode_outputs(x):
            if decode_outputs:
                return self.encdec.dec(x)
            return x
        
        res = {}
        keys_found = set()
        keys_not_found = set()
        with self.db() as db:
            for k in keys:
                v = db.get(_maybe_encode_keys(k))
                if v is None:
                    # key does not exist - set to None
                    keys_not_found.add(k)
                    continue
                else:
                    res[k] = _maybe_decode_outputs(v)
                    keys_found.add(k)
        return res, keys_found, keys_not_found
    

def _merge_chunk(to_upd : dict, d_merge):
    """ this returns the dictionary containing the updated key-value pairs, for the 
    overlapping key-value pairs between `to_upd` and `d_merge`.
    """
    d_m_dict, found, not_found = d_merge.get_many(to_upd.keys())
    # Address and "blocks" require special updates, because they are sets.
    for k in found:
        # link the address to all transactions it has occurred at:
        if 'addr' in k:
            to_upd[k].union(d_m_dict[k])
        if k == 'blocks':
            to_upd[k].union(d_m_dict[k])
    # The not-found keys, are simply sent-over to d_merge:
    to_upd.update({k : to_upd[k] for k in not_found})
    return to_upd # the merged chunk.



class BlockchainKVDB(KVDatabase):
    def buffered_merge(self, other_db: KVDatabase, buffer_size = 200_000):
        """Merges d1 and dm, by applying special merge where required (e.g., w.r.t. addresses)
        and storing the updated key-value pairs to dm
        
        """
        to_upd = {}
        d1 = self
        dm = other_db
        with d1.db() as db:
            for k,v in tqdm(db.iterator()):
                ki, vi = d1.encdec.dec(k), d1.encdec.dec(v)
                to_upd[ki] = vi
                if len(to_upd) == buffer_size:
                    _res = _merge_chunk(to_upd, dm)
                    dm.put_dict(to_upd)
                    to_upd = {}
                
            _res = _merge_chunk({k : v for k , v in to_upd.items()}, dm)
            dm.put_dict(to_upd)
            to_upd = {}
        print("Done merging.")
    def __getitem__(self, key_or_keys):
        if isinstance(key_or_keys,list):
            return self.get_many(key_or_keys)[0]
        return self.get_many([key_or_keys])[0]
    
def _create_key_as_set_or_add(dict_in, key, newval):
    if key not in dict_in:
        dict_in[key] = set()
    dict_in[key].add(newval)

def _create_key_as_list_or_append(dict_in, key, newval):
    if key not in dict_in:
        dict_in[key] = list(newval)
    dict_in[key].append(newval)

def _get_dict_block_data(dat_in, dat_out):
    _buffer_dict = {}
    _buffer_dict['blocks'] = set()
    addr_keys_to_update = set()
    for d_in, d_out in zip(dat_in, dat_out):
        _k_in = (d_in['transaction_hash'],'in','trans')
        _v_in = [i for i in d_in['inputs'].keys()]
        _buffer_dict.update({_k_in : d_in['inputs']})

        _k_out = (d_out['transaction_hash'],'out','trans')
        _v_out = [i for i in d_out['outputs'].keys()]
        _buffer_dict.update({_k_out : d_out['outputs']})

        # Add a link from each UTX to all linked transactions
        for addr in _v_out:
            _key = (addr, 'out','addr')
            _create_key_as_set_or_add(_buffer_dict, _key, d_out['transaction_hash'])
            addr_keys_to_update.add(_key)

        for addr in _v_in:
            _key = (addr, 'in','addr')
            _create_key_as_set_or_add(_buffer_dict, _key, d_out['transaction_hash'])
            addr_keys_to_update.add(_key)
            
        
        _create_key_as_set_or_add(_buffer_dict,'block_height',d_in['transaction_hash'] )
        if d_in['block_height'] not in _buffer_dict:
            _buffer_dict[d_in['block_height']] = set((d_in['transaction_hash'],))
        else:
            _buffer_dict[d_in['block_height']].add(d_in['transaction_hash'])
        
        _buffer_dict['blocks'].add(d_in['block_height'])
    addr_keys_to_update.add('blocks')
    return _buffer_dict, addr_keys_to_update


class BlockChainParse:
    def __init__(self, io_dict_iterator, kv_ldb_file = None):
        if kv_ldb_file is None:
            raise Exception("Please provide a valid LevelDB file.")
        self.io_dict_iterator = io_dict_iterator
        self.kv = KVDatabase(kv_ldb_file)
        
    def parse_merge(self): 
        for dat_in, dat_out in tqdm(self.io_dict_iterator):
            _buffer_dict, utx_keys_to_update = _get_dict_block_data(dat_in, dat_out)
            utx_prev, _, _ = self.kv.get_many(utx_keys_to_update)
            for k, v in utx_prev.items():
                _buffer_dict[k] = _buffer_dict[k].union(v)    
            self.kv.put_dict(_buffer_dict)
        print("done.")
