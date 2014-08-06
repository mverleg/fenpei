
"""
	test feinpei
"""
from fenpei.queue_nijm import NijmQueue

from fenpei.test.job_test import TestJob
from fenpei.queue_local import LocalQueue


def test_jobs():

	jobs = []
	params = set(int(k**1.5) for k in range(5, 20))
	params.add(1)

	for N in params:
		jobs.append(TestJob(
			name = 'test%d' % N,
			substitutions = {TestJob.get_files()[0]: {'N': N}},
			weight = int(N / 10) + 1,
		))

	#queue = LocalQueue()
	queue = NijmQueue()
	queue.all_nodes()
	queue.add_jobs(jobs)
	return queue

if __name__ == '__main__':
	queue = test_jobs()
	queue.run_argv()


