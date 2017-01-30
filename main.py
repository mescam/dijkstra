import sys
import collections
import time
import random
from threading import Thread

import colorama
from mpi4py import MPI
import schedule

comm = MPI.COMM_WORLD
rank = comm.rank
k = comm.size + 1

ILLEGAL_PROBABILITY = 0.02


class LamportClock:
    value = 0

    @staticmethod
    def cmp(x):
        if x > LamportClock.value:
            LamportClock.value = x
        LamportClock.value += 1

    @staticmethod
    def inc():
        LamportClock.value += 1
        return LamportClock.value


class Logger:

    colors = {
        'info': colorama.Fore.GREEN,
        'warning': colorama.Fore.YELLOW,
        'critical': colorama.Fore.RED
    }

    def __init__(self):
        self.rank = comm.rank

    def _message(self, color, message):
        print(colorama.Fore.MAGENTA + "%05d\t" % (LamportClock.value) +
              colorama.Fore.CYAN + ("%02d\t" % self.rank) +
              color + message, file=sys.stderr, flush=True)

    def info(self, message):
        self._message(Logger.colors['info'], message)

    def warning(self, message):
        self._message(Logger.colors['warning'], message)

    def critical(self, message):
        self._message(Logger.colors['critical'], message)


class Message:

    def __init__(self, message):
        self.message = message
        self.lamport = LamportClock.inc()

    def send(self, recipients):
        if isinstance(recipients, collections.Iterable):
            for i in recipients:
                comm.send((self.lamport, self.message), i)
        else:
            comm.send((self.lamport, self.message), recipients)

    def broadcast(self):
        for node in range(comm.size):
            if node != comm.rank:
                comm.send((self.lamport, self.message), node)

    @staticmethod
    def handle(message):
        LamportClock.cmp(message[0])
        return message[1]


class Node:

    def __init__(self):
        self.id = rank
        self.state = 0
        self.next_node = (rank + 1) % comm.size
        self.prev_node = (rank - 1 + comm.size) % comm.size
        self.leader = True if rank == 0 else False
        self.left_state = 0
        self.log = Logger()
        self.sched_t = Thread(target=self.scheduler)

    def change_state(self):
        self.state = (self.state + 1 if self.leader else self.left_state) % k
        self.log.warning('changing state to %d' % self.state)
        Message(self.state).send(self.next_node)

    def can_change_state(self):
        if self.leader:
            return (self.left_state == self.state)
        else:
            return (self.left_state != self.state)

    def do_something(self):
        if random.random() < ILLEGAL_PROBABILITY:
            self.state = (random.randint(0, 1000)) % k
            self.log.critical('I AM NOW AT STATE %d, MWAHAHAHA!' % self.state)
            Message(self.state).send(self.next_node)
        else:
            self.i_want_to_change()

    def i_want_to_change(self):
        if self.can_change_state():
            self.change_state()

    def run(self):
        # initialize stuff
        self.log.info('leader == %d' % int(self.leader))
        schedule.every(1).seconds.do(self.do_something)
        self.sched_t.start()
        self.log.info('node initialized')
        # hey Link, listen
        while True:
            message = Message.handle(comm.recv())
            self.on_receive(message)

    def on_receive(self, left_state):
        self.log.info('node %02d is now at state %d' %
                      (self.prev_node, left_state))
        self.left_state = left_state

    def scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    Node().run()
