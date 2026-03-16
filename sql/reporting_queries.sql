SELECT COUNT(*) FROM transport_report_elt;

SELECT * FROM transport_report_elt LIMIT 10;

SELECT station_name, SUM(passenger_count) AS total_passengers
FROM transport_report_elt
GROUP BY station_name
ORDER BY total_passengers DESC
LIMIT 10;

SELECT line_name, AVG(delay_minutes) AS avg_delay
FROM transport_report_elt
GROUP BY line_name
ORDER BY avg_delay DESC;

SELECT borough_name, SUM(passenger_count) AS total_passengers
FROM transport_report_elt
GROUP BY borough_name
ORDER BY total_passengers DESC;