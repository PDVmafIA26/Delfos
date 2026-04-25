/*
Se haría en un DAG con Airflow y se ejecutaría cada minuto
*/
DELETE FROM outcome_tokens_updates
WHERE ts < extract(epoch from now()) * 1000 - 60000;