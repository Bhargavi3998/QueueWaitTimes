import queue
import scheduler
import logging
import simparser
from datetime import timedelta
from job import JobStatus
import enum


class Mode(enum.IntEnum):
    SECONDS = 1
    MINUTES = 60


class StateInput:
    def __init__(self, curr_time, job_log_lst):
        self.curr_time = curr_time
        self.running_jobs = {}
        self.pending_jobs = {}
        for new_job in job_log_lst:
            if new_job.status == JobStatus.RUNNING:
                self.running_jobs[new_job.job.job_id] = new_job
            elif new_job.status == JobStatus.PENDING:
                self.pending_jobs[new_job.job.job_id] = new_job
            else:
                logging.warning("Invalid jobs for the state reconfiguration")

    def num_jobs(self):
        return len(self.running_jobs) + len(self.pending_jobs)


class StateOutput:
    def __init__(self, curr_time, running_jobs, pending_jobs, cluster_state):
        self.time = curr_time
        self.running_jobs = running_jobs
        self.pending_jobs = pending_jobs
        self.cluster_state = cluster_state


class Simulator:
    def __init__(self, mode=Mode.MINUTES):
        self._jobs = {}
        self._queue = queue.PriorityQueue()
        self._num_submitted_jobs = 0
        self._scheduler = scheduler.Scheduler(0, None, None, None, mode)
        self._time = None
        self._start_time = None
        self._mode = mode

    def job_queue_mapping(self):
        return self._scheduler.job_queue_mapping
    def load_jobs(self, log):
        self._jobs = simparser.SimParser.parse_job_TACC(log)


    def init_scheduler(self, nodes, slurm_config, start_time, backfill_config, queue_time_len=100):
        if "nodes" in slurm_config.keys():
            nodes = slurm_config["nodes"]
        print("Cluster nodes: {}".format(nodes))
        backfill_policy = scheduler.BackfillPolicy(backfill_config, start_time, nodes)
        self._scheduler = scheduler.Scheduler(nodes, start_time, backfill_policy, slurm_config, self._mode,
                                              queue_time_len)
        self._time = start_time
        self._start_time = start_time
        self._step_priority = (slurm_config["PriorityWeightAge"] * int(self._mode) / (
                float(slurm_config["PriorityMaxAge"]) * 24 * 60 * 60))

    def run_end(self, end_job_id=None):

        while True:
            logging.info("Current simulation time: {}".format(self._time))
            if self._queue.empty():
                while True:
                    debug_time = self._time.strftime("%Y-%m-%dT%H:%M:%S")
                    end, end_job_info = self._scheduler.run(timedelta(seconds=int(self._mode)), end_job_id)
                    self._time += timedelta(seconds=int(self._mode))
                    if end_job_id is not None:
                        if end:
                            return end_job_info[1]
                    if len(self._scheduler.job_logs) == self._num_submitted_jobs:
                        return None
            else:
                # get the job from queue and set in submit job array
                next_submit_time, next_job_id, next_new_job, next_queue_name = self._queue.get()  # Adjusted to include queueName

                submit_job = [(next_new_job, next_queue_name)]  # Adjusted to handle job and queueName as a tuple
                while True:
                    if self._queue.empty():
                        break

                    submit_time, job_id, new_job, queue_name = self._queue.get()  # Adjusted to include queueName
                    if next_job_id == '90810':
                        logging.info(f"Skipping Job ID '90810' in run_end.")
                        continue  # Skip this job and proceed with the next one in the queue

                    if submit_time == next_submit_time:
                        submit_job.append((new_job, queue_name))  # Adjusted to handle job and queueName as a tuple
                    else:
                        self._queue.put(
                            (submit_time, job_id, new_job, queue_name))  # Adjusted to re-queue with queueName
                        break
                end, end_job_info = self._scheduler.run(next_submit_time - self._time, end_job_id)

                # Adjust scheduler submission to handle job and queueName
                for job, queue_name in submit_job:
                    logging.info(f"Submitting Job: {job.job_id} in Queue: {queue_name} to Scheduler.")
                    self._scheduler.submit([(job, queue_name)])


                if end:
                    self._time = end_job_info[1] + timedelta(seconds=int(self._mode))
                else:
                    self._time = next_submit_time
                if end_job_id is not None:
                    if end:
                        return end_job_info[1]

    def run_time(self, time):
        #iterate every second/minute for given time
        for _ in range(0, int(time.total_seconds() / int(self._mode))):
            debug_time = self._time.strftime("%Y-%m-%dT%H:%M:%S")
            submit_jobs = []
            # submit jobs whose start time is now
            # if queue is empty stop
            # take first job, if it is time to submit that, add to submit_jobs
            # if the job cannot be submitted send back to queue
            while True:
                if self._queue.empty():
                    break
                submit_time, job_id, new_job = self._queue.get()
                if submit_time == self._time:
                    submit_jobs.append(new_job)
                elif submit_time < self._time:
                    logging.warning(
                        "Error simulation: Job ID: {}, Simulator time: {}, Job submission time: {}".format(job_id,
                                                                                                           self._time,
                                                                                                           submit_time))
                else:
                    self._queue.put((submit_time, job_id, new_job))
                    break
            # ask scheduler to submit and run
            self._scheduler.submit(submit_jobs)
            self._scheduler.run(timedelta(seconds=int(self._mode)))
            self._time += timedelta(seconds=int(self._mode))
        return True

    # It resets the jobs submit time that have submit titme before start time to start time of the simulation
    # and also puts it in queue
    def submit_job_internal(self, job_lst) -> bool:
        success = True
        for job_id in job_lst:
            try:
                # Access the Job object and queueName
                current_job = self._jobs[job_id]['job']
                queue_name = self._jobs[job_id]['queueName']  # Extract queueName

                if self._mode == Mode.MINUTES:
                    current_job.submit = current_job.submit.replace(second=0)
                if current_job.submit < self._time:
                    logging.warning(f"Job submission time is earlier than the simulator time. Job: {job_id}")
                    current_job.submit = self._time

                # Modify this line to include queueName when putting the job in the queue
                # This assumes your queue or scheduler can accept and process this additional information
                self._queue.put((current_job.submit, job_id, current_job, queue_name))

                self._num_submitted_jobs += 1
            except IndexError:
                logging.error(f"Job ID doesn't not exist: {job_id}")
                success = False
        return success

    def find_jobs(self, start_time, end_time) -> []:
        job_lst = []
        for key, value in self._jobs.items():
            # Access the 'job' object and then its 'submit' attribute
            job = value.get('job')  # Safely get the 'job' object
            if job and start_time <= job.submit <= end_time:
                job_lst.append(key)
        return job_lst

    def output_state(self) -> StateOutput:
        return StateOutput(self._time, self._scheduler.running_jobs_state, self._scheduler.queue_state,
                           self._scheduler.cluster_state)

    def avg_queue_time(self):
        return self._scheduler.avg_queue_time()


    @property
    def job_completion_logs(self):
        return self._scheduler.job_logs

    @property
    def sim_time(self):
        return self._time


    @property
    def jobs(self):
        return self._jobs

    @jobs.setter
    def jobs(self, jobs):
        self._jobs = jobs

    @property
    def waiting_queue_size(self):
        return self._scheduler.queue_size

    @property
    def running_job_logs(self):
        return list(self._scheduler.running_jobs_state.values())

    @property
    def pending_job_status(self):
        return self._scheduler.pending_queue
