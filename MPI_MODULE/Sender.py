import pika


def Send(result_matrix):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='returned_result')

    channel.basic_publish(exchange='', routing_key='returned_result', body=str(result_matrix))
    print(" [x] Sent back to client'")
    connection.close()
