"""
    This module enforces access restrictions imposed by the cluster
    architecture: nodes can only run tasks on their own threads. It also
    validates the final result.
    There is a method for checking task execution against the
    list of all threads in the application: node threads and tester threads and
    a method that validates correct termination of all threads
    Access violation errors are stored in a list to be reported at the end of
    a test, together with errors reported by other modules.
    Another method is used to check computed results.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2015
"""

import os, sys

from threading import current_thread, enumerate
from traceback import print_stack


class Supervisor:
    """
    Class used to globally check accesses from node threads and verify result
    correctness.
    """
    def __init__(self, tasks, data, die_on_error = True):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Creates a new empty supervisor.

        @type tasks: List of List of List of (Task, List of (Integer, Integer, Integer), List of (Integer, Integer, Integer))
        @param tasks: the list of all tasks with their input and output slices

        @type data: List of Integer
        @param data: the initial data

        @type die_on_error: Boolean
        @param die_on_error: true for the test to be killed on first error
        """
        self.tasks = tasks
        num_nodes = len(self.tasks[0])  # round 0 always exists, and has tasks for nodes
        chunk_size = len(data) / num_nodes
        self.data  = []
        for i in xrange(num_nodes):
            self.data .append(data[i * chunk_size : (i+1) * chunk_size])
        self.die_on_error = die_on_error
        self.round = 0
        self.nodes = {}
        self.banned_threads = set()
        self.messages = []


    def register_node(self, node, node_id):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Registers a new node with the supervisor.

        @type node: Node
        @param node: the node to register

        @type node_id: Integer
        @param node_id: the node's id (between 0 and N-1)
        """
        self.nodes[node_id] = node


    def register_banned_thread(self, thread = None):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Registers a tester thread. This thread must not be used by nodes for
        any task execution.

        @type thread: Thread
        @param thread: the thread
        """
        if thread == None:
            thread = current_thread()
        self.banned_threads.add(thread)


    def check_execution(self, task):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Check task execution.

        @type task: Task
        @param task: the task
        """
        thread = current_thread()
        if thread in self.banned_threads:
            #ERROR: called from tester thread
            self.report("task scheduled on node '%s' is trying to execute on \
tester thread '%s'" % (str(task._Task__node), thread.name))
            return


    def check_termination(self):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Checks for correct node shutdown. There must not be any active
        node threads.
        """
        for thread in enumerate():
            if thread in self.banned_threads:
                continue
            self.report("thread '%s' did not terminate"
                    % str(thread.name), die_on_error = False)


    def validate(self, crt_round = None):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Validates the current state of the data.
        """
        if crt_round != None:
            self.round = crt_round
        
        task_results = []
        round = self.tasks[self.round]
        for node in round:
            for (task, in_slices, out_slices) in node:
                data = []
                for (n, begin, end) in in_slices:
                    data += self.data[n][begin : end]
                result = task._Task__compute(data)
                task_results.append((result, out_slices))

        for (result, out_slices) in task_results:
            offset = 0
            for (n, begin, end) in out_slices:
                self.data[n][begin : end] = \
                        [x + y for (x, y) in zip(self.data[n][begin : end], result[offset : offset + end - begin])]
                offset += end - begin

        data = {}
        for node_id, node in self.nodes.items():
            data[node_id] = node.get_data()

        for node in self.nodes.keys():
            computed_data = data[node]
            correct_data = self.data[node]
            if len(computed_data) != len(correct_data):
                self.report("length of data for node '%s' changed; expected %i, \
found %i" % (str(node), len(correct_data), len(computed_data)), False)
                continue
            for i in xrange(len(correct_data)):
                if computed_data[i] != correct_data[i]:
                    self.report("data for node '%s' differs at index %i; \
expected %i, found %i" % (str(node), i, correct_data[i], computed_data[i]), False)
        
        self.round += 1


    def report(self, message, die_on_error = None):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Reports an error message. All messages are stored in a list for
        retrieval at the end of the test.

        @type message: String
        @param message: the error message to log
        """
        if die_on_error == None:
            die_on_error = self.die_on_error

        if die_on_error:
            print >> sys.stderr, message + "\n",
            print_stack()
            os._exit(os.EX_SOFTWARE)
        
        self.messages.append(message)


    def status(self):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Returns the list of logged error messages.

        @rtype: List of String
        @return: the list of encountered errors
        """
        return self.messages
