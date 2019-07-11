import time
from statistics import mean, stdev

import math


class CompletionTimeEstimator:
    _max_sample_size = 3000  # about 4hrs of 5-second samples

    sample_size: int
    smoothing_factor: float

    count_history: list
    monotonic_history: list
    rate_history: list

    rate: float
    estimate: float
    uncertainty: float

    def __init__(self):
        self.reset(None, 5, 0.1)

    def reset(self, reason, sample_size=None, smoothing_factor=None):
        if reason is not None:
            print(f'RESETTING ESTIMATED TIME: {reason}')

        if sample_size is not None:
            self.sample_size = sample_size  # auto-increases if there are many repeated measurements
        if smoothing_factor is not None:
            self.smoothing_factor = smoothing_factor  # exponential moving average

        self.count_history = []
        self.monotonic_history = []
        self.rate_history = []

        self.rate = float('nan')
        self.estimate = float('nan')
        self.uncertainty = float('nan')

    def _update_rate(self, instantaneous_rate):
        self.rate_history.append(instantaneous_rate)
        self.rate_history = self.rate_history[-(self.sample_size + 1):]  # housekeeping

        # just use mean rate if less than 5 sample rates
        if len(self.rate_history) < 5:
            new_rate = mean([r for r, t in self.rate_history])

        # use weighted average by duration
        else:
            # total duration
            t0 = self.rate_history[0][-1]
            rate_window_len = self.rate_history[-1][-1] - t0
            assert rate_window_len > 0

            # weighted average
            weighted_rates = []
            for rate, t in self.rate_history[1:]:
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

        if len(self.monotonic_history) == 0:
            assert len(self.count_history) == 0
            self.count_history.append((num_remaining, timestamp))
            self.monotonic_history.append((num_remaining, timestamp))
            return self.estimate

        # calculate delta from last update
        last_n, _ = self.monotonic_history[-1]
        _, last_t = self.count_history[-1]
        assert timestamp > last_t

        # update history
        self.count_history.append((num_remaining, timestamp))
        self.count_history = self.count_history[-(self._max_sample_size * 4 + 1):]  # 400% of max_sample_size

        # item count should not increase
        if num_remaining > last_n:
            self.reset('item count increased')
            self.count_history.append((num_remaining, timestamp))
            self.monotonic_history.append((num_remaining, timestamp))
            return self.estimate  # float('nan')

        # update sample size
        # self.sample_size = min(self.sample_size, 2 * sum(n == num_remaining for n, t in self.count_history))
        self.sample_size = min(self.sample_size, int(len(self.count_history) / 4))  # last 25% of readings
        self.sample_size = max(self._max_sample_size, self.sample_size)

        # update monotonic history
        if num_remaining < last_n:
            self.monotonic_history.append((num_remaining, timestamp))
            self.monotonic_history = self.monotonic_history[-(self.sample_size + 1):]  # housekeeping

            # recalculate rate
            if len(self.monotonic_history) > 1:
                first_n, first_t = self.monotonic_history[0]
                last_n, last_t = self.monotonic_history[-1]
                self._update_rate(((first_n - last_n) / (last_t - first_t), timestamp))

            # check if rate is appropriate given previous info
            if len(self.monotonic_history) > 2:
                prev_n, prev_t = self.monotonic_history[-2]
                expected_n = prev_n - (timestamp - prev_t) * self.rate
                assert expected_n < prev_n

                # remaining amount is more than 20% off from prediction
                if expected_n > 10 and len(self.rate_history) > 10:
                    if abs(num_remaining - expected_n) / expected_n > 0.2:
                        self.reset('significant deviation from expected rate')
                        self.count_history.append((num_remaining, timestamp))
                        self.monotonic_history.append((num_remaining, timestamp))
                        return self.estimate  # float('nan')

        # rate of change could not be estimated
        if math.isnan(self.rate):
            return self.estimate  # float('nan')

        # given the rate, what are the expected end times for past historical counts
        estimates = []
        for c, t in self.count_history[-self.sample_size:]:
            estimates.append(t + c / self.rate)

        # try to keep only future-dated estimates if work remains to be done
        if num_remaining > 0 and any(e > timestamp for e in estimates):
            estimates = [e for e in estimates if e > timestamp]

        # update and return estimated completion time (as timestamp)
        self.estimate = mean(estimates)
        if len(estimates) > 1:
            # self.uncertainty_stdev = max(max(estimates) - self.estimate, self.estimate - min(estimates))
            self.uncertainty = stdev(estimates) * 2  # 2 standard deviations = 95%
        else:
            self.uncertainty = 0
        return self.estimate


class RemainingTimeEstimator:
    eta: float
    estimate: float

    def __init__(self, name=None):
        """
        :type num_remaining: [int, None]
        :type name: [str, None]
        """

        self.CTE = None
        self.eta = float('nan')
        self.estimate = float('nan')
        self.name = name
        self.smoothing_factor = 0.1

    def __str__(self):
        # 2 significant figures of uncertainty
        # noinspection PyStringFormat
        unc_str = f'{{N:,.{1 - int(math.floor(math.log10(self.CTE.uncertainty)))}f}}'.format(N=self.CTE.uncertainty)

        if self.name is None:
            return f'RemainingTime<{self.get_estimate()}±{unc_str}>'
        else:
            return f'RemainingTime<[{self.name}]={self.get_estimate()}±{unc_str}>'

    def update(self, num_remaining):
        """
        :type num_remaining: int
        """
        timestamp = time.time()

        # create new completion time estimator
        if self.CTE is None:
            self.CTE = CompletionTimeEstimator()
            self.CTE.update(num_remaining, timestamp)
            self.estimate = float('nan')
            return self.estimate

        # get estimated time remaining
        completion_estimate = self.CTE.update(num_remaining, timestamp)
        if math.isnan(completion_estimate):
            self.eta = float('nan')
            self.estimate = float('nan')
            return self.estimate

        # moving exponential average for estimate to prevent jumps
        if math.isnan(self.eta):
            self.eta = completion_estimate
        elif self.eta <= timestamp:
            self.eta = max(timestamp, completion_estimate)
        else:
            self.eta = self.eta * self.smoothing_factor + completion_estimate * (1 - self.smoothing_factor)

        # actual remaining time
        self.estimate = self.eta - timestamp

        # uncertainty too high and estimate more than 10 mins
        if self.CTE.uncertainty * 0.1 > self.estimate > 600:  # if either is nan, evaluates to False
            if len(self.CTE.rate_history) > 10:
                print('RESETTING ESTIMATED TIME: uncertainty much greater than estimated time remaining')
                print(f'estimate: {self.estimate}, uncertainty: {self.CTE.uncertainty}')
                self.CTE = CompletionTimeEstimator()
                self.CTE.update(num_remaining, timestamp)
                self.eta = float('nan')
                self.estimate = float('nan')

        return self.get_estimate()

    def get_estimate(self):
        if math.isnan(self.estimate):
            return self.estimate

        if math.isnan(self.CTE.uncertainty):
            return self.estimate

        if self.CTE.uncertainty == 0:
            return self.estimate

        # take 2 standard deviations and round accordingly
        assert self.CTE.uncertainty > 0
        uncertainty_exponent = 10 ** math.floor(math.log10(self.CTE.uncertainty))
        return math.ceil(self.estimate / uncertainty_exponent) * uncertainty_exponent


if __name__ == '__main__':
    a = RemainingTimeEstimator(100)

    time.sleep(0.1)
    print(a.update(100))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(98))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(97))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(97))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(97))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(94))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(95))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(94))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(93))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(93))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(91))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(90))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(90))
    print(a.get_estimate())

    time.sleep(0.1)
    print(a.update(88))
    print(a.get_estimate())
