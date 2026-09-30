cd /home/romaric/Documents/training/AirFlow
source .venv/bin/activate

export AIRFLOW_HOME="$PWD"
export AIRFLOW_CONFIG="$PWD/airflow.cfg"

airflow db migrate
airflow standalone


cd /home/romaric/Documents/training/AirFlow
source .venv/bin/activate
export AIRFLOW_HOME="$PWD"
export AIRFLOW_CONFIG="$PWD/airflow.cfg"

airflow dags list | grep ecommerce

cat "$PWD/simple_auth_manager_passwords.json.generated