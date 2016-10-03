
Fenpei
===============================

This little tool helps in scheduling, tracking and aggregating calculations and their results. It forms the step that brings you from 'a directory with working code for a job' to 'running dozens of jobs and getting results easily'.

    pip install fenpei

This is intended to be used to run multiple intensive computations on a (linux) cluster. At present, it assumes a shared file system on the cluster.

It takes a bit of work to integrate with your situation but it is very flexible and should make your life easier after setting it up. Some features:

* Jobs are created in Python files, making it short and extremely flexible.
* It uses a command line interface (some shell experience required) to easily start, stop or monitor jobs.
* Easy to use with existing code and easily reproducible, since it works by creating isolated job directories.
* Can replaces scheduling queue functionality and start jobs through ssh, or can work with existing systems (slurm and qsum included, others implementable).
* Flexibility for caching, preparation and result extraction.
* Uses multi-processing and can easily use caching for greater performance, and symlinks to save space.

Note that:

* You will have to write Python code for your specific job, as well as any analysis or visualization for the extracted data.
* Except for status monitoring mode, it derives the state on each run, it doesn't keep a database that can get outdated or corrupted.

One example to run reproducible jobs with Fenpei (there are many ways):

* Make a script that runs your code from source to completion for one set of parameters.
* Subclass the ShJobSingle job and add all the files that you need in `get_nosub_files`.
* Replace all the parameters in the run script and other config files by `{{ some_param_name }}`. Add these files to `get_sub_files`.
* Make a Python file (example below) for each analysis you want to run, and fill in all the `some_param_name` with the appropriate values.
* From a shell, use `python your_jobfile.py -s` to see the status, then use other flags for more functionality (see below).
* Implement `is_complete` and `result` in your job (and `crash_reason` if you want `-t`) (others can be overridden too, if you require special behaviour).
* Add analysis code to your job file if you want to visualize the results.

Example file to generate jobs::

    def generate_jobs():
        for alpha in [0.01, 0.10, 1.00]:
            for beta in range(0, 41):
                dict(name='a{0:.2f}_b{1:d}'.format(alpha, beta), subs=dict(
                    alpha=alpha,
                    beta=beta,
                    gamma=5,
                    delta='yes'
                ), use_symlink=True)

    def analyze(queue):
        results = queue.compare_results(('J', 'init_vib', 'init_rot',))
        # You now have the results for all jobs, indexed by the above three parameters.
        # Visualization is up to you, and will be run when the user adds -x

    if __name__ == '__main__':
        jobs = create_jobs(JobCls=ShefJob, generator=generate_jobs(), default_batch=splitext(basename(__file__))[0])
        queue = SlurmQueue(partition='example', jobs=jobs, summary_func=analyze)
        queue.run_argv()

This file registers many jobs for combinations of alpha and beta parameters. You can now use the command line::

    usage: results.py [-h] [-v] [-f] [-e] [-a] [-d] [-l] [-p] [-c] [-w WEIGHT]
                      [-q LIMIT] [-k] [-r] [-g] [-s] [-m] [-x] [-t] [-j]
                      [--jobs JOBS] [--cmd ACTIONS]

    distribute jobs over available nodes

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         more information (can be used multiple times, -vv)
      -f, --force           force certain mistake-sensitive steps instead of
                            failing with a warning
      -e, --restart         with this, start and cleanup ignore complete
                            (/running) jobs
      -a, --availability    list all available nodes and their load (cache reload)
      -d, --distribute      distribute the jobs over available nodes
      -l, --list            show a list of added jobs
      -p, --prepare         prepare all the jobs
      -c, --calc            start calculating one jobs, or see -z/-w/-q
      -w WEIGHT, --weight WEIGHT
                            -c will start jobs with total WEIGHT running
      -q LIMIT, --limit LIMIT
                            -c will add jobs until a total LIMIT running
      -k, --kill            terminate the calculation of all the running jobs
      -r, --remove          clean up all the job files
      -g, --fix             fix jobs, check cache etc (e.g. after update)
      -s, --status          show job status
      -m, --monitor         show job status every few seconds
      -x, --result          run analysis code to summarize results
      -t, --whyfail         print a list of failed jobs with the reason why they
                            failed
      -j, --serial          job commands (start, fix, etc) may NOT be run in
                            parallel (parallel is faster but order of jobs and
                            output is inconsistent)
      --jobs JOBS           specify by name the jobs to (re)start, separated by
                            whitespace
      --cmd ACTIONS         run a shell command in the directories of each job
                            that has a dir ($NAME/$BATCH/$STATUS if --s)

    actions are executed (largely) in the order they are supplied; some actions
    may call others where necessary

Pull requests, extra documentation and bug reports are welcome! It's Revised BSD-licensed so you can do many things.


