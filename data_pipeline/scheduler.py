from crontab import CronTab

cron   = CronTab()
job  = cron.new(command='python ../data_pipeline/run.py DataPipeline --local-scheduler')
job.hour.every(4)