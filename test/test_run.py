
'''
	use scheduler to calculate a lot of networks to compare
'''

from fenpei.job_dnn import DNN_Job
from fenpei.queue_dicp import DICP_Queue
from utility.load_data import read_columns_to_array
from utility.misc import name_weight


batch = 'coord_dnn'
def test_jobs():

	jobs = []
	params = set(k**1.3 for k in range(5, 20))
	print params

	for param in params:
		jobs.append(TestJob(name = 'normal.%s' % (name % rep), weight = weight, data = normal, layers = size, batch_name = batch))

	queue_cluster = DICP_Queue()
	queue_cluster.add_jobs(jobs)
	return queue_cluster

if __name__ == '__main__':
	queue = test_jobs()
	queue.run_argv()


