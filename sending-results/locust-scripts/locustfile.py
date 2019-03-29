import os
import sys

import gevent
import locust
from gevent.queue import Queue
from locust import TaskSet, task, HttpLocust, events, runners
from locust.runners import MasterLocustRunner, SlaveLocustRunner, LocalLocustRunner

##################
# Reading environment variables and setting up constants
#
FEEDER_HOST = os.getenv("FEEDER_HOST", "127.0.0.1")
FEEDER_BIND_PORT = os.getenv("FEEDER_BIND_PORT", 5555)
FEEDER_ADDR = f"tcp://{FEEDER_HOST}:{FEEDER_BIND_PORT}"
QUIET_MODE = True if os.getenv("QUIET_MODE", "true").lower() in ['1', 'true', 'yes'] else False
TASK_DELAY = int(os.getenv("TASK_DELAY", "1000"))


def log(message):
    if not QUIET_MODE:
        print(message)


##################
# Code for detecting run context
#
def is_test_ran_on_master(): return isinstance(runners.locust_runner, MasterLocustRunner)
def is_test_ran_on_slave(): return isinstance(runners.locust_runner, SlaveLocustRunner)
def is_test_ran_on_standalone(): return isinstance(runners.locust_runner, LocalLocustRunner)
def is_locustfile_ran_on_master(): return '--master' in sys.argv
def is_locustfile_ran_on_slave(): return '--slave' in sys.argv
def is_locustfile_ran_on_standalone(): return not ('--slave' in sys.argv or '--master' in sys.argv)


user_count_map = {}
HOST = os.getenv('GRAPHITE_HOST', '127.0.0.1')
PORT = os.getenv('GRAPHITE_PORT', '2003')

forwarder_queue = Queue()


def external_db_forwarder():
    # XXX TODO ideally, connection is separtated.
    # forwarder pops a value and uses a list of adapters to forward it to different sinks
    # (then connection code belong to a sink)
    print(f"-----------connecting to the db on {HOST}, {PORT}")
    # XXX TODO TBD
    pass

    while True:
        data = forwarder_queue.get()
        print(f"db forwarder sending {data}")
        # XXX TODO TBD use elasticsearch adapter to convert and send
        pass


def setup_db_forwarder():
    if is_locustfile_ran_on_master() or is_locustfile_ran_on_standalone():
        print("starting external dv forwarder")
        gevent.spawn(external_db_forwarder)
        locust.events.slave_report += data_producer


def data_producer(client_id, data):
    print(f"data_producer({client_id}, {data})")
    for stat in data["stats"]:
        print(f"stat: {stat}")
        aggregated_stat_message = {"type": "aggregated", "source": client_id, "payload": stat}
        forwarder_queue.put(aggregated_stat_message)


setup_db_forwarder()


class TestBehaviour(TaskSet):
    def on_start(self):
        pass

    @task
    def task1(self):
        log("task1")
        self.client.get("/")


class TestUser(HttpLocust):
    task_set = TestBehaviour
    min_wait = TASK_DELAY
    max_wait = TASK_DELAY


##
def mark(category, message):
    import datetime
    now = datetime.datetime.now()

    new_path = f"./{category}.txt"
    with open(new_path, 'a') as file:
        file.write(f"{now}\t{message}\n")
