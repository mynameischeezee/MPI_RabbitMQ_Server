import pika, sys, os, numpy
from __main__ import *

from Server.StartCalculation import start

DIMENSION = 0
MATRIX_A = []
MATRIX_B = []
ISONSERVER = False
CountOfProcess = 0


def main():
    connectionDimension = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    connectionMatrixA = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    connectionMatrixB = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    connectionIsDataOnServer = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    connectionCountOfProcess = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channelDimension = connectionDimension.channel()
    channelMatrixA = connectionMatrixA.channel()
    channelMatrixB = connectionMatrixB.channel()
    channelIsDataOnServer = connectionIsDataOnServer.channel()
    channelCountOfProcess = connectionCountOfProcess.channel()

    channelDimension.queue_declare(queue='matrix_dimension_queue')
    channelMatrixA.queue_declare(queue='matrix_A_channel')
    channelMatrixB.queue_declare(queue='matrix_B_channel')
    channelIsDataOnServer.queue_declare(queue='is_data_on_server_channel')
    channelCountOfProcess.queue_declare(queue='count_of_process')

    def callbackDimension(ch, method, properties, body):
        global DIMENSION
        DIMENSION = body.decode("utf-8")
        if method.NAME == 'Basic.GetEmpty':
            connectionDimension.close()
            return ''
        else:
            connectionDimension.close()
            return body

    def callbackMatrixA(ch, method, properties, body):
        global MATRIX_A
        MATRIX_A = str(body.decode("utf-8"))
        if method.NAME == 'Basic.GetEmpty':
            connectionMatrixA.close()
            return ''
        else:
            connectionMatrixA.close()
            return body

    def callbackIsDataOnServer(ch, method, properties, body):
        global ISONSERVER
        ISONSERVER = eval(body.decode("utf-8"))
        if method.NAME == 'Basic.GetEmpty':
            connectionIsDataOnServer.close()
            return ''
        else:
            connectionIsDataOnServer.close()
            return body

    def callbackMatrixB(ch, method, properties, body):
        global MATRIX_B
        MATRIX_B = str(body.decode("utf-8"))
        if method.NAME == 'Basic.GetEmpty':
            connectionMatrixB.close()
            return ''
        else:
            connectionMatrixB.close()
            return body

    def callbackCountOfProcess(ch, method, properties, body):
        global CountOfProcess
        CountOfProcess = int(body.decode("utf-8"))
        if method.NAME == 'Basic.GetEmpty':
            connectionCountOfProcess.close()
            return ''
        else:
            connectionCountOfProcess.close()
            return body

    channelDimension.basic_consume(queue='matrix_dimension_queue',
                                   on_message_callback=callbackDimension,
                                   auto_ack=True)
    channelMatrixA.basic_consume(queue='matrix_A_channel',
                                 on_message_callback=callbackMatrixA,
                                 auto_ack=True)
    channelMatrixB.basic_consume(queue='matrix_B_channel',
                                 on_message_callback=callbackMatrixB,
                                 auto_ack=True)
    channelIsDataOnServer.basic_consume(queue='is_data_on_server_channel',
                                        on_message_callback=callbackIsDataOnServer,
                                        auto_ack=True)
    channelCountOfProcess.basic_consume(queue='count_of_process',
                                        on_message_callback=callbackCountOfProcess,
                                        auto_ack=True)

    print('[*] Waiting for messages. To exit press CTRL+C')
    channelDimension.start_consuming()
    channelMatrixA.start_consuming()
    channelMatrixB.start_consuming()
    channelIsDataOnServer.start_consuming()
    channelCountOfProcess.start_consuming()


try:
    main()
    print("[$] Starting calculating with D: " + str(DIMENSION) + " and P: " + str(CountOfProcess))
    start(ISONSERVER, MATRIX_A, MATRIX_B, DIMENSION, CountOfProcess)
except KeyboardInterrupt:
    print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
