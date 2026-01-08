from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime, timedelta
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import logging

# --- Configuration ---

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_TOPICS = ['error_logs', 'warn_logs', 'registration_logs', 'heartbeat_logs']
KAFKA_BROKERS = ['192.168.83.128:9092']
HEARTBEAT_TIMEOUT = 10  # Timeout in seconds to detect missing heartbeats

# SendGrid Configuration
# It's highly recommended to use environment variables for sensitive data like API keys.
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', 'YOUR_SENDGRID_API_KEY')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'you@example.com')
RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAIL', 'you@example.com')

# Shared state for node monitoring: {node_id: {"service_name": str, "last_heartbeat": datetime, "status": str}}
nodes_status = {}

# Helper function to format alert content
def format_error_alert(log_entry):
    timestamp = log_entry.get('timestamp', 'UNKNOWN')
    log_level = log_entry.get('log_level', 'UNKNOWN')
    node_id = log_entry.get('node_id', 'UNKNOWN')
    service_name = log_entry.get('service_name', 'UNKNOWN')
    message = log_entry.get('message', 'No message provided')

    alert_message = f"{timestamp} ALERT: {log_level} | Node: {node_id} | Service: {service_name} | Message: {message}"
    error_details = log_entry.get('error_details', {})
    error_code = error_details.get('error_code', 'N/A')
    error_message = error_details.get('error_message', 'N/A')
    alert_message += f" | Error Code: {error_code} | Details: {error_message}"

    return alert_message

# Function to send an email alert using SendGrid
def send_email_alert(subject, body):
    if not SENDGRID_API_KEY or SENDGRID_API_KEY == 'YOUR_SENDGRID_API_KEY':
        logger.warning("SendGrid API key not configured. Skipping email alert.")
        return

    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=RECIPIENT_EMAIL,
        subject=subject,
        html_content=f'<strong>{body}</strong>'
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info(f"Email alert sent! Status Code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending email alert: {e}")

# Function to process Kafka messages
def consume_logs():
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert-monitor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        topic = message.topic
        log_entry = message.value
        node_id = log_entry.get('node_id')

        if not node_id:
            continue

        if topic == 'heartbeat_logs':
            # Update the last heartbeat time for the node
            if node_id in nodes_status:
                nodes_status[node_id]["last_heartbeat"] = datetime.now()
                # If node was down, mark it as UP again
                if nodes_status[node_id]["status"] == "DOWN":
                    nodes_status[node_id]["status"] = "UP"
                    service_name = nodes_status[node_id]['service_name']
                    logger.info(f"Node {node_id} ({service_name}) is back UP.")
                    # Optional: send a recovery email
                    # send_email_alert(f"RECOVERY: {service_name} is back online", f"Node {node_id} has recovered and is sending heartbeats again.")

        elif topic == 'error_logs':
            # Send a critical alert for ERROR logs
            service_name = log_entry.get('service_name', 'Unknown Service')
            subject = f"CRITICAL ALERT: Error in {service_name}"
            body = format_error_alert(log_entry)
            send_email_alert(subject, body)

        elif topic == 'registration_logs':
            # Track new nodes upon registration
            service_name = log_entry.get('service_name', 'UNKNOWN')
            nodes_status[node_id] = {
                "service_name": service_name,
                "last_heartbeat": datetime.now(),
                "status": "UP"
            }
            logger.info(f"Node {node_id} registered for service {service_name}")

# Function to monitor heartbeat signals
def monitor_heartbeats():
    while True:
        now = datetime.now()
        for node_id, data in list(nodes_status.items()):
            if data["status"] == "UP" and now - data["last_heartbeat"] > timedelta(seconds=HEARTBEAT_TIMEOUT):
                data["status"] = "DOWN"  # Mark as down to prevent duplicate alerts
                service_name = data['service_name']
                subject = f"CRITICAL ALERT: {service_name} is DOWN"
                body = f"Node {node_id} for service {service_name} has stopped sending heartbeats."
                logger.warning(body)
                send_email_alert(subject, body)
        time.sleep(5) # Check every 5 seconds

# Main function to start threads and curses UI
if __name__ == "__main__":
    try:
        # Start the Kafka consumer thread
        consumer_thread = threading.Thread(target=consume_logs, daemon=True)
        consumer_thread.start()

        # Start the heartbeat monitor thread
        heartbeat_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
        heartbeat_thread.start()
        
        logger.info("Alerting system started. Monitoring for critical events...")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
