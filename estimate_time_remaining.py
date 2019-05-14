import time
import warnings

import math


def mean(vec):
    tmp = [elem for elem in vec if elem is not None and not math.isnan(elem)]
    return sum(tmp) / len(tmp)


class CompletionTimeEstimator:

    def __init__(self, num_remaining, timestamp):
        self.sample_size = 20
        self.smoothing_factor = 0.5  # exponential moving average

        self.count_history = [(num_remaining, timestamp)]
        self.moving_count_history = [(num_remaining, timestamp)]
        self.rates = []
        self.estimate = float('nan')

    def update(self, num_remaining, timestamp):

        # calculate delta from last update
        last_n, last_t = self.count_history[-1]
        delta_n = last_n - num_remaining
        delta_t = timestamp - last_t
        assert delta_t > 0

        if delta_n < 0:
            warnings.warn('item count increased, should only decrease')

        # update history
        self.count_history.append((num_remaining, timestamp))
        if delta_n > 0:
            self.moving_count_history.append((num_remaining, timestamp))

            # recalculate rate
            if len(self.moving_count_history) > 1:
                first_n, first_t = self.moving_count_history[-(self.sample_size + 1):][0]
                last_n, last_t = self.moving_count_history[-1]
                self.rates.append(((first_n - last_n) / (last_t - first_t), timestamp))

        # no rate of change logged, stop here
        if not self.rates:
            self.estimate = float('nan')
            return self.estimate

        # just use mean rate if less than 5 sample rates
        if len(self.rates) <= 5:
            rate = mean([r for r, t in self.rates])

        # use weighted average by duration
        else:
            # total duration
            t0 = self.rates[-(self.sample_size + 1):][0][-1]
            rate_window_len = self.rates[-1][-1] - t0
            assert rate_window_len > 0

            # weighted average
            weighted_rates = []
            for rate, t in self.rates[-(self.sample_size + 1):][1:]:
                weighted_rates.append(rate * (t - t0) / rate_window_len)
                t0 = t
            rate = sum(weighted_rates)

        # given the rate, what are the expected end times for past historical counts
        estimates = []
        for c, t in self.count_history[-self.sample_size:]:
            estimates.append(t + c / rate)
            # print(t, c, c/rate)
        # print(estimates)

        # moving exponential average for estimate to prevent jumps
        if math.isnan(self.estimate):
            self.estimate = mean(estimates)
        else:
            self.estimate = self.estimate * self.smoothing_factor + mean(estimates) * (1 - self.smoothing_factor)

        return self.estimate


class RemainingTimeEstimator:
    def __init__(self, num_remaining: [int, None] = None, name: [str, None] = None):
        self.CTE = None
        self.estimate = None
        self.name = name

        if num_remaining:
            self.update(num_remaining)

    def update(self, num_remaining):
        timestamp = time.time()

        if self.CTE is None:
            self.CTE = CompletionTimeEstimator(num_remaining, timestamp)
            self.estimate = float('nan')
            return self.estimate

        completion_time = self.CTE.update(num_remaining, timestamp)
        self.estimate = completion_time - timestamp
        return self.estimate

    def __str__(self):
        if self.name is None:
            return f'RemainingTime<{self.estimate}>'
        else:
            return f'RemainingTime<[{self.name}]={self.estimate}>'
