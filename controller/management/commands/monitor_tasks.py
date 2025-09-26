"""
Usage:
    python manage.py monitor_tasks --interval 120

This command starts the abandoned task monitor daemon with an interval of 120 seconds between checks.
"""

from django.core.management.base import BaseCommand
import threading
import time
from django.conf import settings
from django.utils import timezone
import logging

class Command(BaseCommand):
    help = 'Monitors and processes abandoned tasks.'

    def add_arguments(self, parser):
        # Accept an interval (in seconds) for how frequently the monitor should run.
        parser.add_argument(
            '--interval',
            type=int,
            default=60,
            help='Interval in seconds between checks (default is 60 seconds).'
        )

    def handle(self, *args, **options):
        # Retrieve the user-supplied interval
        logger = logging.getLogger(__name__)
        logger.info("Starting abandoned task monitor daemon...")
        self.interval = options['interval']
        self.stdout.write(f"Starting abandoned task monitor daemon with an interval of {self.interval} seconds...")
        t = threading.Thread(target=self.task_monitor, daemon=True)
        t.start()
        # Keep the command running. In production, might use a more robust solution.
        # For example:
        # - Use a process manager like Supervisor
        # - Use a task queue like Celery

        try:
            while True:
                # time.sleep to keep the main thread alive
                # 1-second delay is typically acceptable to respond to shutdown signals
                time.sleep(1)
        except KeyboardInterrupt:
            self.stdout.write("Daemon stopping...")

    def task_monitor(self):
        # Import here to ensure Django is fully loaded
        import controller.logic.job.data_access_operations as job_dao

        while True:
            print("Checking for abandoned tasks in currently running 3a_kn jobs...")
            # Find all 3a_kn jobs that are running
            list_3a_kn_jobs = job_dao.find_all_jobs(
                job_type=settings.OPERATOR_TYPES[1], # 'human'
                job_name=settings.HUMAN_OPERATORS[0], # '3a_kn
                job_status=settings.JOB_STATUS[1]  # 'RUNNING'
            )
            # For each running job, call abandon_tasks_3a_kn()
            for job in list_3a_kn_jobs:
                print(f"Checking job {job.id}...")
                job_dao.abandon_tasks_3a_kn(obj_job=job)
            print('Finished checking for abandoned tasks in 3a_kn jobs.')

            print("Checking for abandoned tasks in currently running 3a_knlm jobs...")
            # Find all 3a_knlm jobs that are running
            list_3a_knlm_jobs = job_dao.find_all_jobs(
                job_type=settings.OPERATOR_TYPES[1], # 'human'
                job_name=settings.HUMAN_OPERATORS[2], # '3a_knlm
                job_status=settings.JOB_STATUS[1]  # 'RUNNING'
            )
            # For each running job, call abandon_tasks_3a_knlm()
            for job in list_3a_knlm_jobs:
                print(f"Checking job {job.id}...")
                job_dao.abandon_tasks_3a_knlm(obj_job=job)
            print('Finished checking for abandoned tasks in 3a_knlm jobs.')
            
            time.sleep(self.interval)
