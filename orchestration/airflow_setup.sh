export PYTHONPATH=$PYTHONPATH:/usr/local/orchestration

airflow db init

airflow users create \
    --username $AIRFLOW_USER \
    --firstname $AIRFLOW_USER \
    --lastname $AIRFLOW_USER \
    --role Admin \
    --email $AIRFLOW_USER@$AIRFLOW_USER.com \
    --password $AIRFLOW_PASSWORD

airflow connections delete aws_default
echo "Creating AWS connection"
airflow connections add aws_default --conn-login $AWS_ACCESS_KEY_ID --conn-password $AWS_SECRET_ACCESS_KEY --conn-type aws
airflow webserver --port 8080 & # Start the webserver in the background

# Wait for the webserver to start
until $(curl --output /dev/null --silent --head --fail http://localhost:8080); do
    printf '.'
    sleep 5
done

airflow scheduler
