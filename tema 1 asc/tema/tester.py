#!/usr/bin/python

"""
Testing infrastructure - generates and executes tests,
simulates a cluster's FEPs (front-end processors)

Computer Systems Architecture Course
Assignment 1 - Cluster Activity Simulation
March 2015
"""
import pickle, random, sys, os, re, getopt, subprocess

from threading import Timer
from task import Task
from util import *

class Tester:
    """
    Generates test scenarios and simulates the users and front-end-processors
    of a cluster, which send jobs to the nodes.
    """
    def __init__(self, output_filename):
        """
        Constructor.
        @type output_filename: String
        @param output_filename: the file in which the tester logs results
        """
        self.output_filename = output_filename

        self.passed_tests = 0

        self.rand_gen = random.Random()
        self.rand_gen.seed(0)

        self.params_names =[NUM_NODES,
                              TEST_NAME,
                              NUM_ROUNDS,
                              DELAY,
                              TIMEOUT_PERIOD,
                              TASKS_PER_ROUND,
                              INPUT_DATA,
                              OUTPUT_DATA]
        self.child_processes = []

    def run_test(self, testfile, times = 1):
        """
        Performs a test generated from a given file or randomly.
        To better check for synchronization errors the test is run several
        times, as given by the 'times' parameter.
        @type testfile: String
        @param testfile: the path+name of the file containing the test
                        parameters
        @type times: Integer
        @param times: number of time to run the test
        """

        test = self.load_test(testfile)
        self.generate_test_data(test)


        t = Timer(interval=test.timeout,
                    function=Tester.timer_fn,
                    args=(self, test, times))
        t.start()

        print start_test_msg % test.name

        self.passed_tests = 0

        for i in range(times):
            print test_errors_msg % (i + 1, times)

            if self.start_test(test) == 0:
                self.passed_tests += 1
                print "No errors"

        print end_test_msg % test.name

        self.out_file = open(self.output_filename, "a")

        msg = test_finished_msg % (test.name, 100.0*self.passed_tests/times)

        self.out_file.write(msg + "\n")
        self.out_file.close()

        t.cancel()

        self.child_processes = []


    def timer_fn(self, test, num_times):
        """
        Timer function, it's executed in case of timeout
        @param test:
        @param num_times:
        """
        self.out_file = open(self.output_filename, "a")
        if self.passed_tests == num_times:
            msg = test_finished_msg % (test.name, 100.0 * self.passed_tests / times)
        else:
            msg = timout_msg % (test.name, 100.0 * self.passed_tests / times)

        self.out_file.write(msg + "\n")
        self.out_file.close()

        print "TIMEOUT"

        # check if all child process are terminated, and kill those still running
        for p in self.child_processes:
            if p != None and p.poll() == None:
                p.kill()

        os._exit(0)

    def start_test(self, test):
        """
        Starts a child process that will run the test.

        @type test: Test
        @param test: an object containing all the information necessary for
        a test
        """
        path = os.path.dirname(sys.argv[0])
        path = "." if path == "" else path
        command = "python %s/testexec.py" % path
        input = pickle.dumps(test)
        p = subprocess.Popen(command, stdin=subprocess.PIPE, shell=True)
        self.child_processes.append(p)
        p.communicate(input)

        return p.returncode


    def generate_test_data(self, test):
        """
        Updates a test objects with the tasks for each round
        @type test: Test
        @param test: the test for which the tasks are generated
        """

        num_nodes = test.num_nodes

        # generate the array of values
        slice_size = self.rand_gen.randint(20, 1000)
        data_size = num_nodes * slice_size
        data = [self.rand_gen.randint(-1000, 1000) for i in range(data_size)]

        test.set_data(data)

        """
            rounds list: [[tasks_per_node_in_round0]...[tasks_per_node_in_roundn]]
            tasks_per_node_in_round: [[t1,t2, ...], [t1, t2, ...], ...] - a list of tasks for each node
            task: [task_object, input_slices, output_slices]
        """

        num_rounds = len(test.tasks_per_round)
        rounds = []
        for round in range(num_rounds):
            num_tasks = test.tasks_per_round[round]
            l = []
            for node in range(test.num_nodes):
                tasks_per_node = []
                for task in range(num_tasks):
                    tasks_per_node.append(self.create_task(node,
                                                           test.num_nodes,
                                                           slice_size,
                                                           test.input_data_type,
                                                           test.output_data_type,
                                                           test.delay))
                l.append(tasks_per_node)
            rounds.append(l)

        test.set_rounds(rounds)

    def create_task(self, node, num_nodes, slice_size, input_data_type, output_data_type, delay):
        """
        Creates a tuple representing a task for a given node
        @type node: Node
        @param node: the node for which the task is created
        @type num_nodes: Integer
        @param num_nodes: the total number of nodes
        @type slice_size: Integer
        @param slice_size: the size of the sublist held by each node
        @param input_data_type: "LEFT", "RIGHT", "RANDOM" or "SAME_AS_INPUT"
        @param output_data_type: "LEFT", "RIGHT", "RANDOM" or "SAME_AS_INPUT"
        @rtype: list
        @return: [Task object, input slices, output slices]
        """
        input_slices = self.generate_indexes(node, input_data_type, num_nodes, slice_size)
        output_slices = self.generate_indexes(node, output_data_type, num_nodes, slice_size, input_slices)

        return [Task(delay), input_slices, output_slices]

    def generate_indexes(self, node, type, num_nodes, slice_size, input_indexes = None):
        """
        Generates a list of data slices with the format:
        [(Ni, B, E), (Nj, B, E) ...], N - the node index, B and E the begin/end
        indexes relative to the node's data

        @type node: Node
        @param node:
        @type type: str
        @param type: "LEFT", "RIGHT", "RANDOM" or "SAME_AS_INPUT"
        @type num_nodes: Integer
        @param num_nodes: the total number of nodes
        @type slice_size: Integer
        @param slice_size: the size of the sublist held by each node
        @param input_indexes: a list of data slices used as input. This
        parameter is needed when generating the indexes for the output_slices
        @return: a list of slices
        """

        other_nodes_ids = range(0, num_nodes)
        other_nodes_ids.pop(node)

        slices = []

        if input_indexes == None:
            [fi, li] = self.rand_gen.sample(range(0, slice_size - 1), 2)
            if fi > li:
                fi, li = li, fi
        elif len(input_indexes) == 1:
            size = input_indexes[0][2] - input_indexes[0][1]
            fi = self.rand_gen.randint(0, slice_size - size - 1)
            li = fi + size

        if type == "LEFT":
            if node > 0:
                neighbour_id = node -1
            else:
                neighbour_id = num_nodes - 1

            slices.append((neighbour_id, fi, li))

        elif type == "RIGHT":
            if node < num_nodes - 1:
                neighbour_id = node + 1
            else:
                neighbour_id = 0

            slices.append((neighbour_id, fi, li))

        elif type == "SAME_AS_INPUT":
            return input_indexes

        elif type == "RANDOM":
            #pick random a node
            #pick a range in that node

            # for input slices
            if input_indexes == None:
                num_slices = self.rand_gen.randint(1, 10)
                for slice in range(num_slices):
                    neighbour_id = self.rand_gen.choice(other_nodes_ids)
                    [fi, li] = self.rand_gen.sample(range(0, slice_size - 1), 2)
                    if fi > li:
                        fi, li = li, fi
                    slices.append((neighbour_id, fi, li))
            # for output_slices
            # MUST: total size of output slices = total size of input slices
            else:
                num_slices = len(input_indexes)
                for slice in range(num_slices):
                    size = input_indexes[slice][2] - input_indexes[slice][1]
                    neighbour_id = self.rand_gen.choice(other_nodes_ids)
                    fi = self.rand_gen.randint(0, slice_size - 1 - size)
                    slices.append((neighbour_id, fi, fi + size))

        elif type == "SAME_LOC":
            for slice in input_indexes:
                slices.append((0, slice[1], slice[2]))

        return slices

    def load_test(self, test_file):
        """
        Loads the test description from a file with the following format:

        param_name1 = value
        param_name2 = value
        [...]

        Blank lines or lines starting with # (comments) are ignored

        Parameter names are defined in this class. Parameters can be
        declared in any order in the file.

        @param test_file: the test file
        @return: a tuple containing a Test object, a string indicating how
        to generate input data and a string indicating how to generate
        output data (the possible values for this string are described in
        util.py)
        """

        test_params = dict.fromkeys(self.params_names, 0)
        delay = None
        try:
            f = open(test_file, "r")
            for line in f:
                line = line.strip()
                if len(line) == 0 or line.startswith('#'):
                    continue

                parts = [i.strip() for i in re.split("=", line)]
                if len(parts) != 2:
                    raise Exception("Wrong test file format")

                if parts[0] not in test_params:
                    raise Exception("Wrong parameter name: %s" % parts[0])

                if parts[0] == TASKS_PER_ROUND:
                    tasks = parts[1].split(",")

                    if len(tasks) == 1 and parts[1] != tasks[0]:
                        raise Exception("Wrong format for value of parameter \
                                        %s: %s" % parts[0], parts[1])

                    tasks = [int(t) for t in tasks]

                elif parts[0] == INPUT_DATA:
                    input_data_type = parts[1]
                    if input_data_type not in input_slices_sources:
                        raise Exception("Wrong format for specifying input \
                                        data slices : %s"%type)

                elif parts[0] == OUTPUT_DATA:
                    output_data_type = parts[1]
                    if output_data_type not in output_slices_sources:
                        raise Exception("Wrong format for specifying output \
                                        data slices : %s"%type)

                elif parts[0] == TEST_NAME:
                    test_name = parts[1]

                elif parts[0] == DELAY:
                    delay = float(parts[1])

                elif parts[0] == TIMEOUT_PERIOD:
                    timeout = int(parts[1])

                elif parts[0] == NUM_NODES:
                    num_nodes = int(parts[1])

        except IOError, err:
            print err
            os._exit(0)
        except Exception, err:
            print err
            f.close()
            os._exit(0)
        else:
            f.close()

        return (Test(name=test_name,
                    num_nodes=num_nodes,
                    tasks_per_round=tasks,
                    delay=delay,
                    timeout=timeout,
                    input_data_type = input_data_type,
                    output_data_type=output_data_type))

class Test:

    def __init__(self, name="Test", num_nodes=0, tasks_per_round=[], delay = None, timeout=0,
                 input_data_type = "RANDOM", output_data_type = "RANDOM"):
        self.name = name
        self.num_nodes = num_nodes
        self.tasks_per_round = tasks_per_round
        self.delay = delay
        self.timeout = timeout
        self.input_data_type = input_data_type
        self.output_data_type = output_data_type
        self.rounds = []
        self.all_data = []

    def run(self):
        pass

    def set_data(self, data):
        self.all_data = data

    def set_rounds(self, rounds):
        self.rounds = rounds

    def __str__(self):
        s =  "%s\t\nnodes: %d" % (self.name, self.num_nodes) +\
                "\t\nnumber of tasks per round: " + str(self.tasks_per_round)
        if self.rounds != []:
            s += "\t\nrounds' data:\n" + str(self.rounds)
        return s


def usage(argv):
    print "Usage: python %s [OPTIONS]"%argv[0]
    print "options:"
    print "\t-f,   --testfile\ttest file, if not specified it generates a random test"
    print "\t-o,   --out\t\toutput file"
    print "\t-t,   --times\t\tthe number of times the test is run, defaults to 2"
    print "\t-h,   --help\t\tprint this help screen"

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:f:o:t:", ["help",
            "testfile=", "out=", "times="])


        if len(opts) == 0:
            print usage(sys.argv)
            sys.exit(2)

    except getopt.GetoptError, err:
        print str(err)
        usage(sys.argv)
        sys.exit(2)

    test_file = ""
    times = 2
    output_file = "tester.out"

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o in ("-f", "--testfile"):
            test_file = a
        elif o in ("-o", "--out"):
            output_file = a
        elif o in ("-t", "--times"):
            try:
                times = int(a)
            except TypeError, err:
                print str(err)
                sys.exit(2)
        else:
            assert False, "unhandled option"

    t = Tester(output_file)
    t.run_test(test_file, times)
