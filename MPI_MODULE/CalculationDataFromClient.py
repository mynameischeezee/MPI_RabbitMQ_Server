import numpy
import numpy as numpy
from mpi4py import MPI
from random import randint
import sys
import numpy
import os
from environs import Env
from Sender import *

strMatrixA = ""
strMatrixB = ""

with open("../matrixA.txt", "r") as f:
    strMatrixA = f.read()

with open("../matrixB.txt", "r") as f:
    strMatrixB = f.read()

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
workers = comm.Get_size() - 1
CONST_MASTER_RANK = 0
MatrixDimension = int(sys.argv[1])
MatrixA = numpy.reshape(list(map(int, strMatrixA.split(','))), (MatrixDimension, MatrixDimension))
MatrixB = numpy.reshape(list(map(int, strMatrixB.split(','))), (MatrixDimension, MatrixDimension))
ResultMatrix = []
Buffer = MatrixDimension * 5000


def matrix_multiplication(x, y):
    return [[sum(a * b for a, b in zip(X_row, Y_col)) for Y_col in zip(*y)]
            for X_row in x]


def distribute_matrix_data():
    def split_matrix(input_matrix, amount_of_workers):
        rows_split = []
        division_value = len(input_matrix) // amount_of_workers
        leftover = len(input_matrix) % amount_of_workers
        matrix_division_start = 0
        matrix_division_end = division_value + min(1, leftover)
        for i in range(amount_of_workers):
            rows_split.append(input_matrix[matrix_division_start:matrix_division_end])
            leftover = max(0, leftover - 1)
            matrix_division_start = matrix_division_end
            matrix_division_end += division_value + min(1, leftover)
        return rows_split

    rows = split_matrix(MatrixA, workers)
    pid = 1
    for row in rows:
        non_blocking_sending1 = comm.isend(row, dest=pid, tag=1)
        non_blocking_sending1.wait()
        non_blocking_sending2 = comm.isend(MatrixB, dest=pid, tag=2)
        non_blocking_sending2.wait()
        pid = pid + 1


def get_matrix_data():
    global ResultMatrix
    pid = 1
    for n in range(workers):
        non_blocking_row = comm.irecv(buf=Buffer, source=pid, tag=pid)
        row = non_blocking_row.wait()
        ResultMatrix += row
        pid = pid + 1


def master_operation():
    time_start = MPI.Wtime()
    distribute_matrix_data()
    get_matrix_data()
    time_spent = MPI.Wtime() - time_start
    print("[!] master process with #%d finished in: %5.10fs." % (rank, time_spent))
    Send(ResultMatrix)


def slave_operation():
    time_start = MPI.Wtime()
    receive_task1 = comm.irecv(buf=Buffer, source=CONST_MASTER_RANK, tag=1)
    x = receive_task1.wait()
    receive_task2 = comm.irecv(buf=Buffer, source=CONST_MASTER_RANK, tag=2)
    y = receive_task2.wait()
    result = matrix_multiplication(x, y)
    req_send = comm.isend(result, dest=CONST_MASTER_RANK, tag=rank)
    req_send.wait()
    spent_time = MPI.Wtime() - time_start
    print("[!] slave process with #%d finished in: %5.10fs." % (rank, spent_time))



if __name__ == "__main__":
    if rank == CONST_MASTER_RANK:
        master_operation()
    else:
        slave_operation()

