import datetime
import json
import time
import warnings
from typing import Union, Iterable

import math
import pika

from estimate_time_remaining import RemainingTimeEstimator


def format_seconds(num_seconds):
    """
    string formatting
    note that the days in a month is kinda fuzzy
    kind of takes leap years into account, but as a result the years are fuzzy
    :type num_seconds: int | float
    """

    # handle negatives
    if num_seconds < 0:
        minus = '-'
    else:
        minus = ''
    num_seconds = abs(num_seconds)

    # zero (not compatible with decimals below)
    if num_seconds == 0:
        return '0 seconds'

    # 1 or more seconds
    if num_seconds >= 1:
        unit = 0
        denominators = [60.0, 60.0, 24.0, 7.0, 365.25 / 84.0, 12.0]
        while unit < 6 and num_seconds > denominators[unit] * 0.9:
            num_seconds /= denominators[unit]
            unit += 1
        unit = [u'seconds', u'minutes', u'hours', u'days', u'weeks', u'months', u'years'][unit]

        # singular form
        if num_seconds == 1:
            unit = unit[:-1]

        # exact or float
        if num_seconds % 1:
            return f'{minus}{num_seconds:,.2f} {unit}'
        else:
            return f'{minus}{num_seconds:,.0f} {unit}'

    # fractions of a second (ms, μs, ns)
    else:
        unit = 0
        while unit < 3 and num_seconds < 0.9:
            num_seconds *= 1000
            unit += 1
        unit = [u'seconds', u'milliseconds', u'microseconds', u'nanoseconds'][unit]

        # singular form
        if num_seconds == 1:
            unit = unit[:-1]

        # exact or float
        if num_seconds % 1 and num_seconds > 1:
            return f'{minus}{num_seconds:,.2f} {unit}'
        elif num_seconds % 1:
            # noinspection PyStringFormat
            num_seconds = f'{{N:,.{1 - int(math.floor(math.log10(abs(num_seconds))))}f}}'.format(N=num_seconds)
            return f'{minus}{num_seconds} {unit}'
        else:
            return f'{minus}{num_seconds:,.0f} {unit}'


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
    def __init__(self, ip_address, port, virtual_host, username, password, name=None, logfile='rmq.log'):
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
        except Exception:
            print('RMQ connection test failed')
            raise

        self._log({'function': 'init'})

    def __str__(self):
        if self.name is None:
            return f'RMQ<{self.username}@{self.ip_address}:{self.port}/{self.virtual_host}>'
        else:
            return f'RMQ<[{self.name}]={self.username}@{self.ip_address}:{self.port}/{self.virtual_host}>'

    def _log(self, json_data):

        json_data['config'] = {
            'ip_address':   self.ip_address,
            'port':         self.port,
            'virtual_host': self.virtual_host,
            'username':     self.username,
            'timestamp':    datetime.datetime.now().isoformat(),
        }

        if self.logfile is not None:
            for _ in range(5):
                try:
                    with open(self.logfile, mode='at', encoding='utf8', newline='\n') as f:
                        f.write(json.dumps(json_data, indent=4, sort_keys=True, ensure_ascii=False) + '\n')
                        if self.log_separator:
                            f.write(self.log_separator + '\n')
                    break
                except IOError:
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

        if verbose:
            print(f'purging all messages from <{",".join(queue_names)}>')

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
                if auto_ack:
                    print(f'popping {n} messages from <{queue_name}> (total {_num_to_read})')
                else:
                    print(f'peeking at {n} messages in <{queue_name}> (total {_num_to_read})')

            if n > _num_to_read:
                warnings.warn(f'n > queue length, this method blocks until n messages have been read')
            _num_to_read = n

        elif verbose:
            if auto_ack:
                print(f'popping messages from <{queue_name}> (total {_num_to_read})')
            else:
                print(f'peeking at messages in <{queue_name}> (total {_num_to_read})')

        assert type(_num_to_read) is int
        assert _num_to_read >= 0

        self._log({'function':     'read_jsons',
                   'queue_name':   queue_name,
                   'n':            n,
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

        self._log({'function':   'write_jsons',
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

    def wait_until_queues_empty(self, queue_names: Union[str, Iterable[str]], verbose: Union[bool, int, float] = True):
        _eta_max = 999 * 365.25 * 24 * 60 * 60  # 999 years
        _time_start = time.time()
        _completed = set()
        _print_seconds = 40
        _sleep_seconds = 5
        _next_print_time = -1

        if verbose:
            if type(verbose) in (int, float):
                _print_seconds = max(_sleep_seconds, verbose)

        if isinstance(queue_names, str):
            queue_names = [queue_names]

        self._log({'function':    'wait_until_queues_ready',
                   'queue_names': queue_names,
                   })

        # estimator = RemainingTimeEstimator()
        estimators = dict()
        for queue_name in queue_names:
            estimators[queue_name] = RemainingTimeEstimator(name=queue_name)

        while True:
            total_count = 0

            # update all estimators individually
            for queue_name in queue_names:
                queue_count = self.get_count(queue_name)
                assert queue_count >= 0
                total_count += queue_count

                # ignore empty queues
                if queue_count == 0:
                    if queue_name not in _completed:
                        print(f'<{queue_name}> is empty (elapsed {format_seconds(time.time() - _time_start)})')
                        _completed.add(queue_name)
                        del estimators[queue_name]
                    continue

                # queues that somehow got refilled
                if queue_name in _completed:
                    warnings.warn(f'<{queue_name}> unexpectedly refilled!')
                    _completed.remove(queue_name)
                    estimators[queue_name] = RemainingTimeEstimator(name=queue_name)

                # update estimator
                estimators[queue_name].update(queue_count)

            # completed?
            if total_count == 0:
                break

            # print estimated time remaining
            if verbose:
                if time.time() >= _next_print_time:

                    # eta is the worst case estimate
                    furthest_estimate = float('nan')
                    for estimator in estimators.values():
                        furthest_estimate = max(estimator.get_estimate(), furthest_estimate)  # put the nan last

                    # stuff to print
                    unfinished_queues = sorted(queue_name for queue_name in queue_names if queue_name not in _completed)
                    eta = '<?>' if math.isnan(furthest_estimate) else format_seconds(min(_eta_max, furthest_estimate))

                    # print info
                    print(f'waiting for <{",".join(unfinished_queues)}> to be empty... '
                          f'(elapsed {format_seconds(time.time() - _time_start)}, len={total_count}, remaining {eta})')

                    _next_print_time = time.time() + _print_seconds

            # wait a while and then continue
            time.sleep(_sleep_seconds)

    def wait_until_queues_stabilized(self, queue_names, sleep_seconds=30, verbose=True):
        _time_start = time.time()

        if type(queue_names) is str:
            queue_names = [queue_names]

        self._log({'function':    'wait_until_queues_stabilized',
                   'queue_names': queue_names,
                   })

        prev_count = -1
        curr_count = self.get_count(queue_names)
        while prev_count != curr_count:
            if verbose:
                print(f'waiting for <{",".join(queue_names)}> to stabilize... '
                      f'(elapsed {format_seconds(time.time() - _time_start)}, len={curr_count})')
            time.sleep(sleep_seconds)
            prev_count = curr_count
            curr_count = self.get_count(queue_names)

        return curr_count
