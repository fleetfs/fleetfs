import json
import glob
import sys
import time

# Expected arguments are: <results directory> <number of workers>

# Wait for all the result files to be written
while len(glob.glob('{}/*.results'.format(sys.argv[1]))) != int(sys.argv[2]):
    time.sleep(1)

total_bytes = 0
total_ios = 0
max_time_ms = 0
for results in glob.glob('{}/*.results'.format(sys.argv[1])):
    with open(results) as f:
        while True:
            try:
                stats = json.load(f)
                break
            except:
                # Retry until the results are flushed
                time.sleep(1)
        jobs = stats['jobs']
        assert len(jobs) == 1
        job = jobs[0]
        assert job['jobname'] == 'read-test'
        total_bytes += job['read']['io_bytes']
        total_ios += job['read']['total_ios']
        max_time_ms = max(max_time_ms, job['read']['runtime'])

total_MB = total_bytes / 1024**2
secs = max_time_ms / 1000
print('Aggregate stats:')
print('Read (MB/s): {}'.format(int(total_MB / secs)))
print('Read (IO/s): {}'.format(int(total_ios / secs)))