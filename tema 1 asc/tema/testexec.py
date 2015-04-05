import pickle
from threading import Thread
from tester import Test
from node import *
from supervisor import *


def run_test(test, supervisor):
    """
    Runs the test by creating the nodes and scheduling their tasks for each round
    @type test: Test
    @param test:
    @type supervisor: Supervisor
    @param supervisor: the Supervisor object used for checking the threads and
    the validity of the results
    @type: integer
    @return: the number of errors
    """


    # create node objects for this test
    # contains all the Node objects, ordered by index (ascending)
    nodes = []

    slice_size = len(test.all_data)/test.num_nodes

    num_nodes = test.num_nodes
    for i in range(num_nodes):
        nodes.append(Node(node_id=i,
                          data=test.all_data[i*slice_size: (i+1)*slice_size]))
        supervisor.register_node(node=nodes[-1], node_id=i)

    #the nodes need to know about each other

    for node in nodes:
        node.set_cluster_info(nodes=nodes[:])

    for round in test.rounds:

        # send tasks to each node

        for node_id in range(len(round)):
            tasks_list = round[node_id]
            for task in tasks_list:
                #assumed the data provided is correct
                task[0]._Task__set_node(nodes[node_id])
                task[0]._Task__set_supervisor(supervisor)

                nodes[node_id].schedule_task(task[0], task[1], task[2])

        # wait for results from each node

        threads = []
        for node in nodes:
            thread = Thread(name = "Waiter %s" % str(node), target = Node.sync_results, args = (node,))
            supervisor.register_banned_thread(thread)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        supervisor.validate()

        # stop the test if the results are not ok
        if supervisor.status():
            break

    # finish running the test, inform the nodes

    for node in nodes:
        node.shutdown()

    supervisor.check_termination()

    for msg in supervisor.status():
        print >> sys.stderr, msg

    return len(supervisor.status())


if __name__ == "__main__":
    #pickle loads Test objects only if Test is imported
    #see https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled
    test = pickle.loads("".join(sys.stdin.readlines()))
    #print test

    supervisor = Supervisor(test.rounds, test.all_data)

    supervisor.register_banned_thread()
    return_code = run_test(test, supervisor)

    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(return_code)
