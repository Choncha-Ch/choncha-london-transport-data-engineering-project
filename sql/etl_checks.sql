SELECT COUNT(*) FROM transport_report_etl;

SELECT * FROM transport_report_etl LIMIT 10;

SELECT station_name, SUM(passenger_count) AS total_passengers
FROM transport_report_etl
GROUP BY station_name
ORDER BY total_passengers DESC
LIMIT 10;

SELECT line_name, AVG(delay_minutes) AS avg_delay
FROM transport_report_etl
GROUP BY line_name
ORDER BY avg_delay DESC;

SELECT borough_name, SUM(passenger_count) AS total_passengers
FROM transport_report_etl
GROUP BY borough_name
ORDER BY total_passengers DESC;