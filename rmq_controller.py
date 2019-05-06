import datetime
import json
import time
import warnings

import math
import pika


def format_seconds(num):
    """
    string formatting
    note that the days in a month is kinda fuzzy
    :type num: int | float
    """
    num = abs(num)
    if num == 0:
        return u'0 seconds'
    elif num == 1:
        return u'1 second'

    if num < 1:
        # display 2 significant figures worth of decimals
        return (u'%%0.%df seconds' % (1 - int(math.floor(math.log10(abs(num)))))) % num

    unit = 0
    denominators = [60.0, 60.0, 24.0, 7.0, 365.25 / 84.0, 12.0]
    while unit < 6 and num > denominators[unit] * 0.9:
        num /= denominators[unit]
        unit += 1
    unit = [u'seconds', u'minutes', u'hours', u'days', u'weeks', u'months', u'years'][unit]
    return (u'%.2f %s' if num % 1 else u'%d %s') % (num, unit[:-1] if num == 1 else unit)


class RChannel:
    rmq_channel: [pika.adapters.blocking_connection.BlockingChannel, None]
    rmq_conn: [pika.BlockingConnection, None]

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
    def __init__(self, ip_address, port, virtual_host, username, password, logfile='rmq.log', name=None):
        self.ip_address = ip_address
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.logfile = logfile
        self.name = name
        self.log_separator = '--'  # compatible with jdump files

        try:
            with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
                assert rmq_channel.is_open
        except:
            print('RMQ connection test failed')
            raise

        self._log({'function': 'init'})

    def __str__(self):
        insert_name = f'[{self.name}]=' if self.name is not None else ''
        return f'RMQ<{insert_name}{self.username}@{self.ip_address}:{self.port}>'

    def _log(self, json_data):

        json_data['ip_address'] = self.ip_address
        json_data['port'] = self.port
        json_data['virtual_host'] = self.virtual_host
        json_data['username'] = self.username
        json_data['timestamp'] = datetime.datetime.now().isoformat()

        if self.logfile is not None:
            for _ in range(5):
                try:
                    with open(self.logfile, mode='at', encoding='utf8', newline='\n') as f:
                        f.write(json.dumps(json_data, indent=4, sort_keys=True, ensure_ascii=False) + '\n')
                        if self.log_separator:
                            f.write(self.log_separator + '\n')
                    break
                except:
                    time.sleep(1)

    def get_count(self, queue_names):

        if type(queue_names) is str:
            queue_names = [queue_names]

        self._log({'function':    'get_count',
                   'queue_names': queue_names,
                   })

        count = 0
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            for queue_name in queue_names:
                rmq_queue = rmq_channel.queue_declare(queue=queue_name,
                                                      durable=True,
                                                      exclusive=False,
                                                      auto_delete=False,
                                                      passive=True)
                count += rmq_queue.method.message_count

        return count

    def purge(self, queue_names, verbose=True):

        if type(queue_names) is str:
            queue_names = [queue_names]

        self._log({'function':    'purge',
                   'queue_names': queue_names,
                   })

        insert_name = f'<{",".join(queue_names)}>'

        if verbose:
            print(f'purging all messages from {insert_name}')

        removed_count = 0
        with RChannel(self.ip_address, self.port, self.virtual_host, self.username, self.password) as rmq_channel:
            for queue_name in queue_names:
                res = rmq_channel.queue_purge(queue=queue_name)
                assert res.method.NAME == 'Queue.PurgeOk'
                removed_count += res.method.message_count

        return removed_count

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

        self._log({'function':     'read_jsons',
                   'queue_name':  queue_name,
                   '_num_to_read': _num_to_read,
                   })

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

        self._log({'function':    'write_jsons',
                   'queue_name': queue_name,
                   })

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

    def wait_until_queues_ready(self, queue_names, target_value=0, verbose=True, sleep_seconds=30):
        assert target_value >= 0
        assert type(target_value) is int
        num_avg = 10

        t_start = time.time()
        r_e = 'ready' if target_value else 'empty'

        if type(queue_names) is str:
            queue_names = [queue_names]

        self._log({'function':     'wait_until_queues_ready',
                   'queue_names':  queue_names,
                   'target_value': target_value,
                   })

        insert_name = f'<{",".join(queue_names)}>'

        item_count = self.get_count(queue_names)
        deltas = []
        eta = None
        while item_count != target_value:
            if verbose:
                print(f'waiting for {insert_name} to be {r_e}...' +
                      f' (elapsed {format_seconds(time.time() - t_start)}, len={item_count})' +
                      f' (remaining {format_seconds(eta)})' if eta is not None else '')

            time.sleep(sleep_seconds)

            prev_count = item_count
            item_count = self.get_count(queue_names)
            deltas.append(prev_count - item_count)
            if sum(deltas[-3:]):
                eta = sleep_seconds * (item_count - target_value) / (sum(deltas[-num_avg:]) / len(deltas[-num_avg:]))
            else:
                eta = 999 * 365.25 * 24 * 60 * 60  # 999 years

    def wait_until_queues_stabilized(self, queue_names, verbose=True, sleep_seconds=30):
        t = time.time()

        if type(queue_names) is str:
            queue_names = [queue_names]

        self._log({'function':    'wait_until_queues_stabilized',
                   'queue_names': queue_names,
                   })

        insert_name = f'<{",".join(queue_names)}>'

        prev_count = -1
        curr_count = self.get_count(queue_names)
        while prev_count != curr_count:
            if verbose:
                print(f'waiting for {insert_name} to stabilize... '
                      f'(elapsed {format_seconds(time.time() - t)}, len={curr_count})')
            time.sleep(sleep_seconds)
            prev_count = curr_count
            curr_count = self.get_count(queue_names)

        return curr_count
