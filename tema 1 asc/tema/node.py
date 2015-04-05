"""
Mangea Liviu Darius 334CA
Tema 1 ASC

"""


"""
This module represents a cluster's computational node.

Computer Systems Architecture Course
Assignment 1 - Cluster Activity Simulation
March 2015
"""

from reentrant_barrier import *

class MyThread(Thread):
    def __init__(self, task, in_slices, out_slices, nodes):
        Thread.__init__(self)
        self.task = task
        self.in_slices = in_slices
        self.out_slices = out_slices
        self.nodes = nodes
        self.gather_list = []
        self.recv_data = []
        self.computed_list = []
        self.to_send = []
    
    """ In primul for sunt aduse datele necesare. Acestea sunt apoi computate.
    In al doilea for se transmit datele calculate catre noduri """    
    def run(self):
        
        for tuplu in self.in_slices:
            del self.recv_data[:]       
            self.recv_data = self.nodes[tuplu[0]].get_data()
            self.gather_list.extend(self.recv_data[tuplu[1]:tuplu[2]])
            
        self.computed_list = self.task.run(self.gather_list)
        
        for tuplu in self.out_slices:
            del self.to_send[:]
            self.to_send.extend(self.computed_list[0:tuplu[2]-tuplu[1]])
            del self.computed_list[0:tuplu[2] - tuplu[1]]
            self.nodes[tuplu[0]].send_data(self.to_send, tuplu[1], tuplu[2])
            


class Node:
    """
    Class that represents a cluster node with computation and storage
    functionalities.
    """

    def __init__(self, node_id, data):
        """
        Constructor.

        @type node_id: Integer
        @param node_id: the unique id of this node; between 0 and N-1

        @type data: List of Integer
        @param data: a list containing this node's data
        """
        self.node_id = node_id
        self.data = data
        self.nodes = None
        self.thread_list = []
        self.thread_wait = []
        self.partial_data = [x for x in data]
        

    def __str__(self):
        """
        Pretty prints this node.

        @rtype: String
        @return: a string containing this node's id
        """
        return "Node %d" % self.node_id
    
    """ Din nodul cu id 0 se trimit lockul si bariera catre celelalte noduri """
    def set_cluster_info(self,  nodes):
        """
        Informs the current node about the other nodes in the cluster.
        Guaranteed to be called before the first call to 'schedule_task'.

        @type nodes: List of Node
        @param nodes: a list containing all the nodes in the cluster
        """
        self.nodes = nodes
        
        if self.node_id == 0:
            self.lock = Lock()
            self.barrier = ReusableBarrierSem(len(self.nodes))
            for i in range(1, len(self.nodes)):
                self.nodes[i].receive(self.lock, self.barrier)
                
    """ Nodul primeste bariera si lockul de la nodul 0 """
    def receive(self, lock, barrier):
        self.lock = lock
        self.barrier = barrier
    
    """ Aici se primesc datele care trebuie adunate la vectorul de date """    
    def send_data(self, recv, start, end):
        self.lock.acquire()
        for i in range(start, end):
            self.partial_data[i] = self.partial_data[i] + recv[i - start]
        self.lock.release()
        
        
    """ Se creaza threadurile si se pornesc daca sunt mai putin de 16 active,
    altfel sunt puse intr-o lista pentru a fi pornite mai tarziu """
    def schedule_task(self, task, in_slices, out_slices):
        """
        Schedule task to execute on the node.

        @type task: Task
        @param task: the task object to execute

        @type in_slices: List of (Integer, Integer, Integer)
        @param in_slices: a list of the slices of data that need to be
            gathered for the task; each tuple specifies the id of a node
            together with the starting and ending indexes of the slice; the
            ending index is exclusive

        @type out_slices: List of (Integer, Integer, Integer)
        @param out_slices: a list of slices where the data produced by the
            task needs to be scattered; each tuple specifies the id of a node
            together with the starting and ending indexes of the slice; the
            ending index is exclusive
        """
        
        t = MyThread(task, in_slices, out_slices, self.nodes)
        
        if len(self.thread_list) < 16:
            self.thread_list.append(t)
            t.start()
        else:
            self.thread_wait.append(t)
        
    """ Dupa ce s-a facut join pe toate nodurile in while, se asteapta la
    bariera ca toate nodurile sa ajunga acolo si se updateaza vectorul data """
    def sync_results(self):
        """
        Wait for scheduled tasks to finish and write results.
        """

        while (len(self.thread_list) + len(self.thread_wait)) > 0:
            self.thread_list[0].join()
            self.thread_list.pop(0)
            
            if len(self.thread_wait) > 0:
                th = self.thread_wait.pop(0)
                self.thread_list.append(th)
                th.start()
                
        self.barrier.wait()
        
        del self.thread_list[:]
        
        self.data = [x for x in self.partial_data]


    def get_data(self):
        """
        Return a copy of this node's data.
        """
        return [x for x in self.data]


    def shutdown(self):
        """
        Instructs the node to shutdown (terminate all threads). This method
        is invoked by the tester. This method must block until all the threads
        started by this node terminate.
        """
        pass
