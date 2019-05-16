import time
import warnings

import math


def mean(vec):
    tmp = [elem for elem in vec if elem is not None and not math.isnan(elem)]
    if len(tmp):
        return float(sum(tmp) / len(tmp))
    else:
        return float('nan')


class CompletionTimeEstimator:
    rate: float
    estimate: float

    def __init__(self, num_remaining, timestamp):
        """
        :param num_remaining: number of items left to process
        :type num_remaining: int
        :param timestamp: unix/windows timestamp or time.time()
        :type timestamp: [int, float]
        """

        self.sample_size = 5  # auto-increases if there are many repeated measurements
        self.smoothing_factor = 0.5  # exponential moving average

        self.count_history = [(num_remaining, timestamp)]
        self.monotonic_history = [(num_remaining, timestamp)]
        self.rate_history = []

        self.rate = float('nan')
        self.estimate = float('nan')

    def _update_rate(self):
        if not self.rate_history:
            self.rate = float('nan')
            return self.rate

        # just use mean rate if less than 5 sample rates
        if len(self.rate_history) < 5:
            new_rate = mean([r for r, t in self.rate_history])

        # use weighted average by duration
        else:
            # total duration
            t0 = self.rate_history[-(self.sample_size + 1):][0][-1]
            rate_window_len = self.rate_history[-1][-1] - t0
            assert rate_window_len > 0

            # weighted average
            weighted_rates = []
            for rate, t in self.rate_history[-(self.sample_size + 1):][1:]:
                weighted_rates.append(rate * (t - t0) / rate_window_len)
                t0 = t
            new_rate = sum(weighted_rates)
            assert new_rate > 0

        # moving exponential average for rate to prevent jumps
        if math.isnan(self.rate):
            self.rate = new_rate
        else:
            self.rate = self.rate * self.smoothing_factor + new_rate * (1 - self.smoothing_factor)

        # not really necessary to return, but why not
        return self.rate

    def update(self, num_remaining, timestamp):
        """
        :param num_remaining: number of items left to process
        :type num_remaining: int
        :param timestamp: unix/windows timestamp or time.time()
        :type timestamp: [int, float]
        """

        # calculate delta from last update
        last_n, _ = self.monotonic_history[-1]
        _, last_t = self.count_history[-1]
        assert timestamp > last_t

        # update history
        self.count_history.append((num_remaining, timestamp))

        # item count should not increase
        if num_remaining > last_n:
            warnings.warn('item count increased, it should only decrease')
            self.sample_size += 1

        # update sample size
        self.sample_size = min(self.sample_size, 2 * sum(n == num_remaining for n, t in self.count_history))
        self.sample_size = min(self.sample_size, int(len(self.count_history) / 4))  # last 25% of readings
        self.sample_size = max(self.sample_size, 3000)  # about a day of 30-second samples

        # update monotonic history
        if num_remaining < last_n:
            self.monotonic_history.append((num_remaining, timestamp))

            # recalculate rate
            if len(self.monotonic_history) > 1:
                first_n, first_t = self.monotonic_history[-(self.sample_size + 1):][0]
                last_n, last_t = self.monotonic_history[-1]
                self.rate_history.append(((first_n - last_n) / (last_t - first_t), timestamp))
                self._update_rate()

        # rate of change could not be estimated
        if math.isnan(self.rate):
            self.estimate = float('nan')
            return self.estimate

        # given the rate, what are the expected end times for past historical counts
        estimates = []
        for c, t in self.count_history[-self.sample_size:]:
            estimates.append(t + c / self.rate)

        # try to keep only future-dated estimates if work remains to be done
        if num_remaining > 0 and any(e > timestamp for e in estimates):
            estimates = [e for e in estimates if e > timestamp]

        # housekeeping
        # self.count_history = self.count_history[-(self.sample_size + 1):]  # needed for sample size update
        self.monotonic_history = self.monotonic_history[-(self.sample_size + 1):]
        self.rate_history = self.rate_history[-(self.sample_size + 1):]

        # update and return estimated completion time (as timestamp)
        self.estimate = mean(estimates)
        return self.estimate


class RemainingTimeEstimator:
    eta: float
    estimate: float

    def __init__(self, num_remaining=None, name=None):
        """
        :type num_remaining: [int, None]
        :type name: [str, None]
        """

        self.CTE = None
        self.eta = float('nan')
        self.estimate = float('nan')
        self.name = name
        self.smoothing_factor = 0.1

        if num_remaining:
            self.update(num_remaining)

    def update(self, num_remaining):
        """
        :type num_remaining: int
        """
        timestamp = time.time()

        # create new completion time estimator
        if self.CTE is None:
            self.CTE = CompletionTimeEstimator(num_remaining, timestamp)
            self.estimate = float('nan')
            return self.estimate

        # get estimated time remaining
        completion_estimate = self.CTE.update(num_remaining, timestamp)

        # moving exponential average for estimate to prevent jumps
        if math.isnan(self.eta):
            self.eta = completion_estimate
        elif self.eta <= timestamp:
            self.eta = max(completion_estimate, timestamp)
        else:
            self.eta = self.eta * self.smoothing_factor + completion_estimate * (1 - self.smoothing_factor)

        # actual remaining time
        self.estimate = self.eta - timestamp
        return self.estimate

    def __str__(self):
        if self.name is None:
            return f'RemainingTime<{self.estimate}>'
        else:
            return f'RemainingTime<[{self.name}]={self.estimate}>'
