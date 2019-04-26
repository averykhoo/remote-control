import json

import pika


class RChannel:
    rmq_channel: pika.adapters.blocking_connection.BlockingChannel
    rmq_conn: pika.BlockingConnection

    def __init__(self, ip_address, port, virtual_host, username, password):
        self.parameters = pika.ConnectionParameters(host=ip_address,
                                                    port=port,
                                                    virtual_host=virtual_host,
                                                    credentials=pika.credentials.PlainCredentials(username, password),
                                                    heartbeat=60)
        self.rmq_conn = None
        self.rmq_channel = None

    def __enter__(self):
        self.rmq_conn = pika.BlockingConnection(parameters=self.parameters)
        self.rmq_channel = self.rmq_conn.channel()
        return self.rmq_channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.rmq_channel is not None:
            self.rmq_channel.close()
            self.rmq_channel = None

        if self.rmq_conn is not None:
            self.rmq_conn.close()
            self.rmq_conn = None


class RMQ:
    def __init__(self, ip_address, port, virtual_host, username, password, name=None):
        self.ip_address = ip_address
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.name = name

    def get_count(self, queue_name):
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            rmq_queue = rmq_channel.queue_declare(queue=queue_name,
                                                  durable=True,
                                                  exclusive=False,
                                                  auto_delete=False,
                                                  passive=True)
            return rmq_queue.method.message_count

    def purge(self, queue_name):
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            ret = rmq_channel.queue_purge(queue=queue_name)
            # assert ret.method.NAME == 'Queue.PurgeOk'
            # return ret.method.message_count
            return ret

    def read_json(self, queue_name, n=-1, auto_ack=False, timeout_seconds=60):
        assert type(n) is int

        if n != 0:
            with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
                for i, (method_frame, header_frame, body) in enumerate(
                        rmq_channel.consume(queue=queue_name, inactivity_timeout=timeout_seconds)):
                    if body is None:
                        continue

                    if type(body) is bytes:
                        body = body.decode('utf8')

                    yield json.loads(body)

                    if auto_ack and method_frame:
                        rmq_channel.basic_ack(method_frame.delivery_tag)

                    if n >= 0 and n <= i + 1:
                        break

                # re-queue unacked messages, if any
                rmq_channel.cancel()

    def write_jsons(self, queue_name, json_iterator):
        n_inserted = 0
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            for json_obj in json_iterator:
                rmq_channel.basic_publish(exchange='',
                                          routing_key=queue_name,
                                          body=json.dumps(json_obj,
                                                          ensure_ascii=False,
                                                          sort_keys=True,
                                                          allow_nan=False))
                n_inserted += 1

        return n_inserted
