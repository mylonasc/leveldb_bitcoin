import os
from queue import Queue, Empty
from threading import Thread
from time import sleep
from src.utils import BlockchainKVDB
import shutil

BUFFER_SIZE = 200_000 # how many keys to read/write at once while merging. 

# Mock LevelDB database wrapper class
class DatabaseWrapper:
    def __init__(self, db_file):
        self.db_file = db_file
    
    def _init_db(self):
        self.db = BlockchainKVDB(self.db_file)
        
    def merge(self, other, output_file):
        """
        Function to merge this database with another database.
        Creates a new database file as the output.
        """
        print(f"Merging {self.db_file} and {other.db_file} into {output_file}")
        self._init_db()
        # other._init_db()
        print("...copying")
        shutil.copytree(other.db_file, output_file)
        dbw = DatabaseWrapper(output_file)
        dbw._init_db()
        self.db.buffered_merge(dbw.db, buffer_size = BUFFER_SIZE)
        # db_out = DatabaseWrapper(output_file)        
        return dbw
    
from hashlib import md5

def worker(job_queue, output_file_prefix, thread_id):
    """
    Worker function to process the merge jobs from the queue.
    """
    while True:
        try:
            # Try to get two databases from the queue
            db1 = job_queue.get_nowait()
            try:
                db2 = job_queue.get_nowait()
            except Empty:
                # If there's no second database, put db1 back and exit
                job_queue.put(db1)
                job_queue.task_done()  # Mark db1 as "processed"
                break
            
            m1 = md5(db1.db_file.encode()).hexdigest()
            m2 = md5(db2.db_file.encode()).hexdigest()
            # Create a unique filename for the merged output
            output_file = f"{output_file_prefix}_intermediate_{thread_id}_{job_queue.qsize()}_{m1}_{m2}.db"
            
            # Merge the databases
            merged_db = db1.merge(db2, output_file)
            
            # Add the merged result back to the queue
            job_queue.put(merged_db)
            
            # Signal that the jobs are done
            job_queue.task_done()
            job_queue.task_done()
        except Empty:
            # Exit loop if the queue is empty
            break

def reduce_databases_with_queue(databases, output_file_prefix, num_workers=4):
    """
    Reduce a list of DatabaseWrapper instances into a single database
    using a concurrent job queue.
    """
    # Create a thread-safe queue
    job_queue = Queue()
    
    # Enqueue all databases
    for db in databases:
        job_queue.put(db)
    
    # Create worker threads
    threads = []
    for i in range(num_workers):
        thread = Thread(target=worker, args=(job_queue, output_file_prefix, i))
        thread.start()
        threads.append(thread)
    
    # Wait for all tasks to be processed
    for thread in threads:
        thread.join()  # Wait for each thread to finish
    
    # Ensure only one database remains in the queue
    remaining_items = []
    while not job_queue.empty():
        remaining_items.append(job_queue.get())
        job_queue.task_done()  # Mark as done for remaining items
    
    if len(remaining_items) != 1:
        raise ValueError("Reduction failed; there should be exactly one merged database at the end.")
    
    # Return the final merged database
    final_db = remaining_items[0]
    return final_db


# Example usage
if __name__ == "__main__":
    # Create some mock databases
    # databases = [
    #     DatabaseWrapper("db1.db"),
    #     DatabaseWrapper("db2.db"),
    #     DatabaseWrapper("db3.db"),
    #     DatabaseWrapper("db4.db"),
    # ]
    
    databases = [
        'leveldb_files/kv_db_0_to_10.ldb',
        'leveldb_files/kv_db_10_to_20.ldb',
        'leveldb_files/kv_db_20_to_30.ldb',
        'leveldb_files/kv_db_40_to_50.ldb',
        'leveldb_files/kv_db_50_to_60.ldb',
        'leveldb_files/kv_db_60_to_70.ldb',
        'leveldb_files/kv_db_70_to_80.ldb',
        'leveldb_files/kv_db_80_to_90.ldb',
        'leveldb_files/kv_db_90_to_100.ldb',
    ]
    databases = [DatabaseWrapper(d) for d in databases]
    
    # Perform the reduction
    final_database = reduce_databases_with_queue(databases, "merged", num_workers=16)
    print(f"Final merged database: {final_database.db_file}")
