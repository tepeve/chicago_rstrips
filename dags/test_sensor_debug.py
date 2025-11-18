from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.db import provide_session
from airflow.utils.state import DagRunState
from datetime import datetime
import pendulum

@dag(
    dag_id="test_sensor_debug",
    schedule=None,
    start_date=pendulum.now(),
    catchup=False,
    tags=["debug"]
)
def test_sensor():
    
    @task
    def check_coldstart_runs():
        """Muestra todos los runs del coldstart para debugging."""
        @provide_session
        def get_runs(session=None):
            runs = session.query(DagRun).filter(
                DagRun.dag_id == "coldstart_etl_pipeline"
            ).order_by(DagRun.execution_date.desc()).all()
            
            print(f"\n{'='*60}")
            print(f"Runs encontrados para coldstart_etl_pipeline: {len(runs)}")
            print(f"{'='*60}\n")
            
            for run in runs:
                print(f"Run ID: {run.run_id}")
                print(f"  Execution Date: {run.execution_date}")
                print(f"  State: {run.state}")
                print(f"  Start Date: {run.start_date}")
                print(f"  End Date: {run.end_date}")
                print(f"  Run Type: {run.run_type}")
                print(f"-" * 60)
            
            return len(runs)
        
        return get_runs()
    
    check_coldstart_runs()

test_sensor()