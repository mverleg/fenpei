
"""
	test feinpei
"""

from fenpei.queue_local import LocalQueue
from fenpei.test.job_test import TestJob


def test_jobs():

	jobs = []
	params = set(int(k**1.7) for k in range(1, 15))

	for N in params:
		jobs.append(TestJob(
			name = 'test%d' % N,
			subs = {'N': N},
			weight = int(N / 10) + 1,
		))

	queue = LocalQueue()
	queue.all_nodes()
	queue.add_jobs(jobs)
	return queue


if __name__ == '__main__':
	queue = test_jobs()
	queue.run_argv()


