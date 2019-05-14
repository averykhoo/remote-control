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

        self.sample_size = 6
        self.smoothing_factor = 0.5  # exponential moving average

        self.count_history = [(num_remaining, timestamp)]
        self.moving_count_history = [(num_remaining, timestamp)]
        self.rate_history = []

        self.rate = float('nan')
        self.estimate = float('nan')

    def update(self, num_remaining, timestamp):
        """
        :param num_remaining: number of items left to process
        :type num_remaining: int
        :param timestamp: unix/windows timestamp or time.time()
        :type timestamp: [int, float]
        """

        # calculate delta from last update
        last_n, last_t = self.count_history[-1]
        delta_n = last_n - num_remaining
        delta_t = timestamp - last_t
        assert delta_t > 0

        # item count should not increase
        if delta_n < 0:
            warnings.warn('item count increased, it should only decrease')

        # update history
        self.count_history.append((num_remaining, timestamp))
        if delta_n > 0:
            self.moving_count_history.append((num_remaining, timestamp))

            # recalculate rate
            if len(self.moving_count_history) > 1:
                first_n, first_t = self.moving_count_history[-(self.sample_size + 1):][0]
                last_n, last_t = self.moving_count_history[-1]
                self.rate_history.append(((first_n - last_n) / (last_t - first_t), timestamp))

        # no rate of change logged, stop here
        if not self.rate_history:
            self.estimate = float('nan')
            return self.estimate

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

        # moving exponential average for rate to prevent jumps
        if math.isnan(self.rate):
            self.rate = new_rate
        else:
            self.rate = self.rate * self.smoothing_factor + new_rate * (1 - self.smoothing_factor)

        # given the rate, what are the expected end times for past historical counts
        estimates = []
        for c, t in self.count_history[-self.sample_size:]:
            estimates.append(t + c / self.rate)

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
