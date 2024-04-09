import csv
import sys
import os
# Add the path to the 'sim' module to the Python path
sim_module_path = os.path.abspath("/Users/bhargavidwivedi/Documents/Research/Simulator/src")
sys.path.append(sim_module_path)

import json
import numpy as np
import job
from datetime import datetime
from datetime import timedelta


class SimParser:

    def parse_job_TACC(log, pivot=None, job_filter=None):
        job_dict = {}
        with open(log) as f:
            reader = csv.reader(f, delimiter=',')
            next(reader,None)
            for row in reader:
                if row[0].strip() == '90810' or row[0].strip() == '108114'or row[0].strip() == '103943' or row[0].strip() =='102933':
                    continue
                start_time = datetime.strptime(row[3].strip(), "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(row[4].strip(), "%Y-%m-%d %H:%M:%S")
                submit_time = datetime.strptime(row[5].strip(), "%Y-%m-%d %H:%M:%S")
                queueName=row[6].strip()
                if job_filter is not None:
                    if any(True for filter_item in job_filter if filter_item[0] <= submit_time < filter_item[1]):
                        continue
                if (end_time - start_time).total_seconds() > (int(row[7]) * 60):
                    end_time = start_time + timedelta(seconds=int(row[7]) * 60)
                new_job = job.Job(row[0], int(row[10]), start_time,
                                  submit_time, end_time, int(row[7]))
                if pivot is not None:
                    if new_job.duration.total_seconds() / (new_job.time_limit * 60) < pivot:
                        continue
                job_dict[row[0]] = {'job': new_job, 'queueName': queueName}


        return job_dict


    @staticmethod
    def load_slurm_config(config_file):
        with open(config_file) as f:
            return json.load(f)

    @staticmethod
    def load_backfill_config(config_file):
        with open(config_file) as f:
            return json.load(f)
