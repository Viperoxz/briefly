from dagster import ScheduleDefinition
from ..jobs import sources_topics_job, articles_update_job


sources_topics_schedule = ScheduleDefinition(
    job=sources_topics_job,
    cron_schedule="58 * * * *",  
    execution_timezone="Asia/Ho_Chi_Minh"
)

articles_update_schedule = ScheduleDefinition(
    job=articles_update_job,
    cron_schedule="0 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    run_config={"ops": {"articles": {"config": {"save_json": False}}}}
)