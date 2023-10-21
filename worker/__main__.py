import os
import threading
import socket
import json
import time
import sh
import click
from mapreduce.utils import getabspath, merge_files


class Worker:
    WORKER_THREADS = []
    PID = os.getpid()
    shut_down = False

    def startup(self, master_port_num, worker_port_num):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", worker_port_num))

        sock.listen(5)

        # Create socket to register worker
        reg_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect to the server
        reg_sock.connect(("localhost", master_port_num))
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": worker_port_num,
            "worker_pid": self.PID
            }

        message = json.dumps(message_dict)
        reg_sock.sendall(message.encode('utf-8'))
        reg_sock.close()

        sock.settimeout(1)
        self.listen_tcp(sock, master_port_num)

    def handle_job(self, job, master_port_num):
        """Handle new task from master. Bind thread here."""
        if job['message_type'] == "new_worker_job":
            task_done = self.handle_map_reduce(job)
        else:
            task_done = self.handle_grouping(job)
        # Tell master that job is done
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", master_port_num))
        message = json.dumps(task_done)
        sock.sendall(message.encode('utf-8'))
        sock.close()

    def handle_map_reduce(self, job):
        """Handle mapping and reducing tasks."""
        outfiles = []
        for infile in job['input_files']:
            filename = infile.split('/')[-1]

            fullinpath = getabspath(infile)
            fulloutpath = getabspath(job['output_directory'], filename)
            fullexecpath = getabspath(job['executable'])

            executable = sh.Command(fullexecpath)
            with open(fullinpath) as input_file:
                with open(fulloutpath, "w+") as output_file:
                    executable(_in=input_file, _out=output_file)
            outfiles.append(job['output_directory'] + filename)

        return ({
            "message_type": "status",
            "output_files": outfiles,
            "status": "finished",
            "worker_pid": self.PID
        })

    def handle_grouping(self, job):
        """Handle grouping task."""
        merge_files(job['input_files'], job['output_file'])

        return ({
            "message_type": "status",
            "output_file": job['output_file'],
            "status": "finished",
            "worker_pid": self.PID
        })

    def listen_tcp(self, sock, master_port_num):
        """Listen for input with new sock."""
        while not self.shut_down:
            try:
                clientsocket, _ = sock.accept()
            except socket.timeout:
                continue

            message_chunks = []
            while True:
                try:
                    data = clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)
            clientsocket.close()

            # Decode list-of-byte-strings to UTF8 and parse JSON data
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)

            # Switch on message_type to handle worker behavior
            if message_dict['message_type'] == 'register_ack':
                # Create UDP heartbeat thread
                heartbeat_thread = threading.Thread(
                                        target=self.create_heartbeat,
                                        args=(master_port_num - 1,)
                                        )
                heartbeat_thread.start()
                self.WORKER_THREADS.append(heartbeat_thread)
            elif message_dict['message_type'] == 'shutdown':
                self.shut_down = True
            elif message_dict['message_type'].endswith("_job"):
                job_thread = threading.Thread(
                                        target=self.handle_job,
                                        args=(message_dict, master_port_num,)
                                        )
                job_thread.start()
                self.WORKER_THREADS.append(job_thread)

    def create_heartbeat(self, port_num):
        while not self.shut_down:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            message_dict = {
                "message_type": "heartbeat",
                "worker_pid": self.PID
                }
            message = json.dumps(message_dict)
            sock.sendto(message.encode('utf-8'), ('localhost', port_num))
            sock.close()
            time.sleep(2)


@click.command()
@click.argument("master_port_num", nargs=1, type=int)
@click.argument("worker_port_num", nargs=1, type=int)
def main(master_port_num, worker_port_num):
    worker = Worker()
    worker.startup(master_port_num, worker_port_num)
    print("Worker shutting down")


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
