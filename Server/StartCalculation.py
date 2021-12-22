import os
import sys


def start(isDataOnServer, matrixA, matrixB, matrixDimension, countOfProcess):
    if isDataOnServer:
        com = 'mpiexec -n ' + str(countOfProcess) + ' ' + str(
            sys.executable) + ' ' + 'D:\EDU\Sem7\ParallelAndDistributedComputing\Labs\Lab8\MPI_MODULE\CalculationDataOnServer.py' + ' ' + matrixDimension
    else:
        with open("../matrixA.txt", "w") as f:
            f.write(matrixA)
        with open("../matrixB.txt", "w") as f:
            f.write(matrixB)
        com = 'mpiexec -n ' + str(countOfProcess) + ' ' + str(
            sys.executable) + ' ' + 'D:\EDU\Sem7\ParallelAndDistributedComputing\Labs\Lab8\MPI_MODULE\CalculationDataFromClient.py' + ' ' + matrixDimension
    os.system(com)
