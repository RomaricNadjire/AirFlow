## Créer une connexion pour AirFlow
```bash
airflow connections add postgres_ecommerce \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-login airflow_training \
  --conn-password airflow_training_password \
  --conn-schema ecommerce
```