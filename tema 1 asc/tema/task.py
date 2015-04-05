"""
    This module implementes the tasks that will run on the cluster.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2015
"""

from random import uniform
from threading import current_thread
from time import sleep


class Task:
    def __init__(self, delay = None):
        """
        !!! This is not part of the assignment API, do not call it !!!

        Creates a new task.
        """
        self.__delay = delay
        self.__supervisor = None
        self.__node = None


    def run(self, data):
        """
        Executes this task.

        @type data: List of Integer
        @param data: list containing gathered input data

        @rtype: List of Integer
        @return: a list containing computed data
        """
        self.__supervisor.check_execution(self)
        
        if self.__delay != None:
            sleep(self.__delay * uniform(0.9, 1.1))

        return self.__compute(data)


    def __compute(self, data):
        return [x + 2 for x in data]

    def __set_node(self, node):
        self.__node = node

    def __set_supervisor(self, supervisor):
        self.__supervisor = supervisor
