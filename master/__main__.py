import time
import os
import glob
import threading
import socket
import json
from collections import deque, Counter
from shutil import rmtree, copytree
import click
from mapreduce.utils import getabsoutdir, getabspath, merge_files


class Master():
    """Master class."""

    master_threads = []
    shutdown_bool = False
    workers = {}
    job_counter = 0
    current_job = 0
    job_queue = deque()
    ready = True
    # Denotes what stage the current job is in
    # 0 --> mapping, 1 --> grouping, 2 --> reducing
    current_stage = 0
    status_counter = Counter()
    status_count = float("inf")
    mapper_files = set()
    grouping_files = set()
    reducing_files = set()

    def startup(self, port_num):
        dir_path = getabspath("tmp")
        if os.path.exists(dir_path):
            to_delete = glob.glob(dir_path + "/job-*")
            for path in to_delete:
                rmtree(path)
        else:
            os.makedirs(dir_path)

        # Create UDP heartbeat thread
        heartbeat_thread = threading.Thread(target=self.listen_udp,
                                            args=(port_num - 1,))
        heartbeat_thread.start()
        self.master_threads.append(heartbeat_thread)

        # Create job queue thread
        queue_thread = threading.Thread(target=self.check_queue)
        queue_thread.start()
        self.master_threads.append(queue_thread)

        # Create TCP listener - wait for messages!
        self.listen_tcp(port_num)

    def handle_dead_worker(self, dead_pid):
        """Reassign all tasks, remove pid from worker."""
        alive_workers = []
        num_workers = 0
        while num_workers == 0:
            for pid, worker in self.workers.items():
                if worker['alive']:
                    alive_workers.append(pid)
            num_workers = len(alive_workers)
            time.sleep(.5)

        count = 0
        for task in self.workers[dead_pid]["current_tasks"]:
            task['worker_pid'] = alive_workers[count % num_workers]
            self.workers[alive_workers
                         [count % num_workers]
                         ]["current_tasks"].append(task)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(
                    (self.workers[alive_workers[count % num_workers]]['host'],
                     self.workers[alive_workers[count % num_workers]]['port'])
                )
                message = json.dumps(task)
                sock.sendall(message.encode('utf-8'))
            except socket.error:
                continue
            sock.close()

            count += 1

        self.status_counter[dead_pid] = 0

    def increment_miss_count(self):
        """Increment misses, check if any worker misses > 5 heartbeats."""
        while not self.shutdown_bool:
            time.sleep(2)
            just_died = []
            for pid, worker in self.workers.items():
                if worker['alive']:
                    worker['miss_count'] += 1
                    if worker['miss_count'] > 5:
                        worker['alive'] = False
                        just_died.append(pid)
            for pid in just_died:
                self.handle_dead_worker(pid)

    def listen_udp(self, port):
        """Listen for heartbeat messages from workers. Thread here."""
        time_2 = threading.Thread(target=self.increment_miss_count)
        time_2.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))

        sock.settimeout(1)

        while not self.shutdown_bool:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue

            if data:
                message_str = data.decode("utf-8")
                message_dict = json.loads(message_str)
                if (message_dict['message_type'] == 'heartbeat' and
                        message_dict['worker_pid'] in self.workers):
                    self.workers[message_dict['worker_pid']]['miss_count'] = 0

    def map(self, job, workers):
        """Partition input and assign work to ready workers."""
        input_files = os.listdir(job['input_directory'])
        num_mappers = job['num_mappers']
        # Init list of tasks: these will be JSON messages
        # To be sent to ready workers
        tasks = []
        count = 0

        # Start by appending a task message for each map task requested
        # If you have more files than tasks,
        # Append a file to preexisting task file lists and distribute equally
        for file in input_files:
            fullname = job['input_directory'] + file
            if count >= num_mappers:
                tasks[count % num_mappers]['input_files'].append(fullname)
            else:
                odir = "tmp/job-{}/mapper-output/".format(self.current_job)
                task = {
                    "message_type": "new_worker_job",
                    "input_files": [fullname],
                    "executable": job['mapper_executable'],
                    "output_directory": odir,
                    "worker_pid": 0   # Will be assigned later
                }
                tasks.append(task)
            count += 1

        # status_count is set to the number tasks sent out by the mapper when
        # in the mapping stage
        self.status_count = len(tasks)
        # Now assign work equally, send tasks
        count = 0
        num_workers = len(workers)

        for task in tasks:
            task['worker_pid'] = workers[count % num_workers]
            self.workers[
                workers[count % num_workers]
            ]["current_tasks"].append(task)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(
                    (self.workers[workers[count % num_workers]]['host'],
                     self.workers[workers[count % num_workers]]['port'])
                )
                message = json.dumps(task)
                sock.sendall(message.encode('utf-8'))
            except socket.error:
                continue
            sock.close()

            count += 1

    def worker_group(self, workers):
        """Assign worker to mergesort and then mergesorts those files."""
        num_workers = len(workers)
        file_partitions = [[] for _ in range(num_workers)]
        count = 0
        for file in self.mapper_files:
            file_partitions[count % num_workers].append(file)
            count += 1

        count = 0
        while count < num_workers:
            task = {
                "message_type": "new_sort_job",
                "input_files": file_partitions[count],
                "output_file": "tmp/job-{}/grouper-output/grouped".format(
                    self.current_job) + str(count),
                "worker_pid": workers[count]
            }

            self.workers[workers[count]]['current_tasks'].append(task)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(
                    (self.workers[workers[count % num_workers]]['host'],
                     self.workers[workers[count % num_workers]]['port'])
                )
                message = json.dumps(task)
                sock.sendall(message.encode('utf-8'))
            except socket.error:
                continue
            sock.close()
            count += 1

        self.status_count = len(workers)

    def master_group(self):
        outfile = f"tmp/job-{self.current_job}/grouper-output/fully_grouped"
        merge_files(self.grouping_files, outfile)

    def create_partitions(self, job):
        lines = []
        with open(
                getabsoutdir(self.current_job, 'g', "fully_grouped"), 'r'
        ) as infile:
            lines.extend(infile.read().split('\n'))

        output_file_content = []
        [prev_key, _] = lines[0].split("\t")
        count = 0
        output_file_content.append([lines[0]])
        for i in range(1, len(lines)):
            [key, _] = lines[i].split("\t")
            if key != prev_key:
                output_file_content.append([lines[i]])
                count += 1
                prev_key = key
            else:
                output_file_content[count].append(lines[i])

        output_path = getabsoutdir(self.current_job, 'g')

        count = 0
        # Append the keys to the files
        for key_lines in output_file_content:
            fulloutpath = output_path + "reduce" + str(
                (count % job['num_reducers']) + 1
            )
            with open(fulloutpath, 'a+') as outfile:
                outfile.write('\n'.join(key_lines))
                outfile.write('\n')
            count += 1

    def reduce(self, job, workers):
        """Partition input and assign work to ready workers."""
        self.create_partitions(job)
        path = getabsoutdir(self.current_job, 'g')
        input_files = glob.glob(path + "/reduce*")
        # Init list of tasks: these will be JSON messages
        tasks = []
        count = 0

        # Start by appending a task message for each map task requested
        # If you have more files than tasks,
        # Append a file to preexisting task file lists and distribute equally
        for file in input_files:
            fullname = "tmp/job-{}/grouper-output/".format(
                self.current_job
                ) + file.split("/")[-1]
            if count >= job['num_reducers']:
                tasks[count % job['num_reducers']]['input_files'].append(
                    fullname
                    )
            else:
                odir = "tmp/job-{}/reducer-output/".format(
                    self.current_job
                )
                task = {
                    "message_type": "new_worker_job",
                    "input_files": [fullname],
                    "executable": job['reducer_executable'],
                    "output_directory": odir,
                    "worker_pid": 0   # Will be assigned later
                }
                tasks.append(task)
            count += 1

        # status_count is set to the number tasks sent out by the mapper when
        # in the mapping stage
        self.status_count = len(tasks)
        # Now assign work equally, send tasks
        count = 0
        num_workers = len(workers)

        for task in tasks:
            task['worker_pid'] = workers[count % num_workers]
            self.workers[
                workers[count % num_workers]
            ]["current_tasks"].append(task)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(
                    (self.workers[workers[count % num_workers]]['host'],
                     self.workers[workers[count % num_workers]]['port'])
                )
                message = json.dumps(task)
                sock.sendall(message.encode('utf-8'))
            except socket.error:
                continue
            sock.close()

            count += 1

    def assign_work(self, job, workers_list):
        """Pivot function based on current stage."""
        if self.current_stage == 0:
            self.map(job, workers_list)
        elif self.current_stage == 1:
            self.worker_group(workers_list)
        elif self.current_stage == 2:
            self.master_group()
            self.reduce(job, workers_list)

    def check_queue(self):
        """Attach thread to this process to check if there is work to do."""
        current_job = {}
        while not self.shutdown_bool:
            workers_list = []
            workers_available = (
                True in [
                    worker['alive'] for worker in self.workers.values()
                    ]
                )
            if (self.job_queue and self.ready and workers_available):
                self.ready = False
                job = self.job_queue.popleft()
                current_job = job

                for pid, worker in self.workers.items():
                    if worker["alive"]:
                        workers_list.append(pid)

                self.assign_work(job, workers_list)

            for pid, worker in self.workers.items():
                if worker["alive"]:
                    workers_list.append(pid)

            total_completed = 0
            for pid, num_completed in self.status_counter.items():
                total_completed += num_completed

            if total_completed == self.status_count:
                for pid, worker in self.workers.items():
                    worker['current_tasks'] = []
                self.current_stage += 1
                self.status_count = 0
                self.status_counter = Counter()
                if self.current_stage > 2:
                    self.finish_job(current_job['output_directory'])
                else:
                    self.assign_work(job, workers_list)
            # Want a long sleep here to avoid spinning because
            # 1. we can ignore a job for <1 second
            # 2. job queue will be empty for a long time
            #       during startup and shutdown
            time.sleep(0.75)

    def shutdown(self):
        """Shutdown master and workers."""
        self.shutdown_bool = True
        shutdown_msg = '{"message_type": "shutdown"}'

        for pid, worker in self.workers.items():
            if worker['alive']:
                shutdown_sock = socket.socket(socket.AF_INET,
                                              socket.SOCK_STREAM)
                try:
                    shutdown_sock.connect((self.workers[pid]['host'],
                                           self.workers[pid]['port']))
                    shutdown_sock.sendall(shutdown_msg.encode('utf-8'))
                except socket.error:
                    continue
                shutdown_sock.close()

    def register(self, message_dict):
        """Add new worker to workers, send reg ack."""
        new_worker = {
            "host": message_dict['worker_host'],
            "port": message_dict['worker_port'],
            "miss_count": 0,
            "alive": True,
            "current_tasks": []
        }
        self.workers[message_dict['worker_pid']] = new_worker

        ack_dict = {
            "message_type": "register_ack",
            "worker_host": message_dict['worker_host'],
            "worker_port": message_dict['worker_port'],
            "worker_pid": message_dict['worker_pid']
        }
        ack_message = json.dumps(ack_dict)
        ack_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ack_sock.connect((message_dict['worker_host'],
                          message_dict['worker_port']))
        ack_sock.sendall(ack_message.encode('utf-8'))
        ack_sock.close()

    def new_job(self, request):
        """Build dir structure for new job, add to job queue."""
        dir_path = getabspath("tmp", "job-{}".format(self.job_counter))

        os.makedirs(dir_path)
        os.makedirs(dir_path + "/mapper-output/")
        os.makedirs(dir_path + "/grouper-output/")
        os.makedirs(dir_path + "/reducer-output/")

        job_entry = {
            "job_id": self.job_counter,
            "input_directory": request['input_directory'],
            "output_directory": request['output_directory'],
            "mapper_executable": request['mapper_executable'],
            "reducer_executable": request['reducer_executable'],
            "num_mappers": request['num_mappers'],
            "num_reducers": request['num_reducers']
        }
        self.job_queue.append(job_entry)
        self.job_counter += 1

    def status(self, message_dict):
        """Handle a status tcp message."""
        if self.current_stage == 0:
            for file in message_dict['output_files']:
                self.mapper_files.add(file)
        elif self.current_stage == 1:
            self.grouping_files.add(message_dict['output_file'])
        elif self.current_stage == 2:
            for file in message_dict['output_files']:
                self.mapper_files.add(file)
        self.status_counter[message_dict['worker_pid']] += 1

    def finish_job(self, output_dir):
        """Move files to output_dir, rename files, prepare for new job."""
        reducer_path = getabsoutdir(self.current_job, 'r')
        output_path = getabspath(output_dir) + '/'

        if os.path.exists(output_path):
            rmtree(output_path)
        copytree(reducer_path, output_path)

        # Rename files
        output_files = os.listdir(output_path)
        file_count = 1
        for file in output_files:
            os.rename(output_path + file,
                      output_path + "outputfile{}".format(file_count))
            file_count += 1

        self.status_count = float("inf")
        self.status_counter = Counter()
        self.current_stage = 0
        print("Done with job {}!".format(self.current_job))
        self.current_job += 1
        self.ready = True

    def listen_tcp(self, port):
        """Listen for input with new sock."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))

        sock.listen(5)

        sock.settimeout(1)

        while not self.shutdown_bool:
            try:
                client, _ = sock.accept()
            except socket.timeout:
                continue

            message_chunks = []
            client.settimeout(1)
            while True:
                try:
                    data = client.recv(4096)
                except socket.timeout:
                    # This would be the timeout from shutdown
                    break
                if not data:
                    break
                message_chunks.append(data)

            client.close()

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)

            if message_dict['message_type'] == 'shutdown':
                self.shutdown()
            elif message_dict['message_type'] == 'register':
                self.register(message_dict)
            elif message_dict['message_type'] == 'new_master_job':
                print("New job for master!")
                self.new_job(message_dict)
            elif message_dict['message_type'] == 'status':
                self.status(message_dict)


@click.command()
@click.argument("port_num", nargs=1, type=int)
def main(port_num):
    """Execute Master class."""
    master = Master()
    master.startup(port_num)
    print("Master shutting down")


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
