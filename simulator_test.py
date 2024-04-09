import unittest

import sys
sys.path.append('/Users/bhargavidwivedi/Documents/Research/Simulator/src/sim')

import simulator
import simparser


class SimulatorTestCase(unittest.TestCase):
    def test(self):
        from datetime import datetime

        job_log = "/Users/bhargavidwivedi/Documents/Research/Rich_TACC_Dataset/smoldata2.csv"
        slurm_config = "/Users/bhargavidwivedi/Documents/Research/queueWaitTimeSlurmSim/slurm_config.json"
        backfill_config = "/Users/bhargavidwivedi/Documents/Research/queueWaitTimeSlurmSim/backfill_config.json"
        slurm_config = simparser.SimParser.load_slurm_config(slurm_config)
        backfill_config = simparser.SimParser.load_backfill_config(backfill_config)
        simulator_tmp = simulator.Simulator(mode=simulator.Mode.MINUTES)
        simulator_tmp.load_jobs(job_log)
        start_time = datetime.strptime("2019-08-04T21:17:00", "%Y-%m-%dT%H:%M:%S")

        simulator_tmp.init_scheduler(100, slurm_config, start_time, backfill_config)
        # In test_basic_TACC function, replace the problematic line with the following:


        jobs_log = simulator_tmp.find_jobs(datetime.strptime("2019-10-03T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                                           datetime.strptime("2019-10-14T23:59:59", "%Y-%m-%dT%H:%M:%S"))
        print("*****************")
        target_date_str = "10/5/2019 11:10:11 AM"
        target_date = datetime.strptime(target_date_str, "%m/%d/%Y %I:%M:%S %p")

        # Filter jobs_log to exclude the job with the target start date
        filtered_jobs_log = [job_id for job_id in jobs_log if simulator_tmp._jobs[job_id]['job'].submit != target_date]

        # Remove the job from simulator_tmp._jobs to ensure it's not processed in run_end()
        for job_id in jobs_log:
            if simulator_tmp._jobs[job_id]['job'].submit == target_date:
                del simulator_tmp._jobs[job_id]

        # Submit and process the remaining jobs
        simulator_tmp.submit_job_internal(filtered_jobs_log)
        simulator_tmp.run_end()
        # one job
        print("------------------------------------------Job Logs-----------------------------------------------------")

        queue_wait_times = {}

        # Loop through each job completion log
        for item in simulator_tmp.job_completion_logs:
            job_id = item.job.job_id
            queue_name = simulator_tmp._scheduler.job_queue_mapping.get(job_id, "Unknown Queue")
            wait_time = item.job.wait_time
            # Add the job's wait time to its queue's total
            if queue_name in queue_wait_times:
                queue_wait_times[queue_name] += wait_time
            else:
                queue_wait_times[queue_name] = wait_time

            # Optional: Print details for each job
            print("************************", wait_time)
            print(f"Queue Name: {queue_name}")
            print(
                f"Job ID: {job_id}, Job Submit: {item.job.submit}, Job Start: {item.start}, Job End: {item.end}, Job Nodes: {item.nodes}")

        # Print the total wait times for each queue
        print("-------------------------------------------------------------------------------------------------------")
        print("Total Wait Times by Queue:")
        for queue, total_wait in queue_wait_times.items():
            print(f"Queue '{queue}': Total Wait Time = {total_wait} seconds")
        print("-------------------------------------------------------------------------------------------------------")



if __name__ == '__main__':
    unittest.main()
