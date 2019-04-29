import json
import warnings

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

        try:
            with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
                assert rmq_channel.is_open
        except:
            print('RMQ connection test failed')
            raise

    def __str__(self):
        insert_name = f'[{self.name}]=' if self.name is not None else ''
        return f'RMQ<{insert_name}{self.username}@{self.ip_address}:{self.port}'

    def get_count(self, queue_name):
        if type(queue_name) is str:
            queue_names = [queue_name]
        else:
            queue_names = queue_name

        count = 0
        for q_name in queue_names:
            with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
                rmq_queue = rmq_channel.queue_declare(queue=q_name,
                                                      durable=True,
                                                      exclusive=False,
                                                      auto_delete=False,
                                                      passive=True)
                count += rmq_queue.method.message_count

        return count

    def purge(self, queue_name, verbose=True):
        if verbose:
            print(f'purging all messages in {queue_name} queue')

        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            res = rmq_channel.queue_purge(queue=queue_name)
            assert res.method.NAME == 'Queue.PurgeOk'
            return res.method.message_count

    def read_jsons(self, queue_name, n=None, auto_ack=False, timeout_seconds=60, verbose=True):
        # how many to read from mq
        _num_to_read = self.get_count(queue_name)
        if n is not None:
            if verbose:
                print(f'reading {n} messages from <{queue_name}> queue (total {_num_to_read}); auto_ack={auto_ack}')
            if n > _num_to_read:
                warnings.warn(f'n > queue length, this method blocks until n messages have been read')
            _num_to_read = n
        elif verbose:
            print(f'reading all messages from <{queue_name}> queue (total {_num_to_read}); auto_ack={auto_ack}')
        assert type(_num_to_read) is int

        # start reading
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            for method_frame, header_frame, body in rmq_channel.consume(queue=queue_name,
                                                                        inactivity_timeout=timeout_seconds):
                # finished reading messages
                if _num_to_read == 0:
                    break

                # rabbit mq way of saying there's nothing left (after timeout_seconds of the queue being empty)
                if body is None:
                    continue

                # decode to utf8
                if type(body) is bytes:
                    body = body.decode('utf8')

                # json decode
                yield json.loads(body)

                # ack message
                if auto_ack and method_frame:
                    rmq_channel.basic_ack(method_frame.delivery_tag)

                # count down until n==0
                _num_to_read -= 1

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
