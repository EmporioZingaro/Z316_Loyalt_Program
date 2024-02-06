import os
import logging
import time
import datetime
from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sendgrid.helpers.mail import Asm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

# Initialize BigQuery Client
bq_client = bigquery.Client()

# SendGrid setup
SENDGRID_TOKEN = os.getenv('SENDGRID_TOKEN')
sg_client = SendGridAPIClient(SENDGRID_TOKEN)
template_id = 'd-9c2512bde9eb49deb8e578bfcc0a1054'
from_email = 'sac@emporiozingaro.com'

# Test mode settings
TEST_MODE = False
TEST_EMAIL = 'rodrigo@brunale.com'
EMAIL_SEND_LIMIT = 5

# Adjusted query with test logic
query = """
SELECT c.cpf_cnpj, c.email, cd.nome, cd.final_tier, cd.data_pedido, cd.pedido_number,
       cd.nome_vendedor, cd.totalVenda, cd.cashback
FROM `emporio-zingaro.z316_tiny.z316-tiny-contatos` as c
JOIN `emporio-zingaro.z316_tiny.z316_commission_details_23Q4` as cd
ON c.cpf_cnpj = cd.cpf
"""

def fetch_and_process_data():
    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        # Mapping for final_tier translation
        final_tier_mapping = {
            'Platinum': 'Platina',
            'Bronze': 'Bronze',
            'Silver': 'Prata',
            'Gold': 'Ouro'
        }

        clients_data = {}
        for row in results:
            client_id = row['cpf_cnpj']
            if client_id not in clients_data:
                # Translate the final_tier value
                translated_final_tier = final_tier_mapping.get(row['final_tier'], row['final_tier'])

                clients_data[client_id] = {
                    'client_name': row['nome'],
                    'email': row['email'],
                    'purchase_details': [],
                    'cashback_total': 0,
                    'quarter_spend': 0,
                    'daily_checkin_dates': set(),
                    'final_tier': translated_final_tier
                }

            formatted_date = row['data_pedido'].strftime('%Y-%m-%d')

            # Add all purchase details
            clients_data[client_id]['purchase_details'].append({
                'date': formatted_date,
                'order_number': row['pedido_number'],
                'seller': row['nome_vendedor'],
                'value': "{:.2f}".format(row['totalVenda'])
            })

            # Sum cashback and quarter spend
            clients_data[client_id]['cashback_total'] += row['cashback']
            clients_data[client_id]['quarter_spend'] += row['totalVenda']

            # Check if this is the first purchase of the day for daily check-in
            if formatted_date not in clients_data[client_id]['daily_checkin_dates']:
                clients_data[client_id]['daily_checkin_dates'].add(formatted_date)

        # Finalize data formatting
        for client_id, data in clients_data.items():
            data['daily_checkin_total'] = len(data['daily_checkin_dates'])
            del data['daily_checkin_dates']  # Remove the set as it's no longer needed
            data['quarter_spend'] = "{:.2f}".format(data['quarter_spend'])
            data['lifetime_spend'] = data['quarter_spend']
            data['cashback'] = "{:.2f}".format(data['cashback_total'])

            logging.info(f"Final data for {client_id}: {data}")

        return clients_data

    except Exception as e:
        logging.error(f"Error fetching or processing data: {e}")
        return {}

def send_email(client_data, retry_count=0):
    try:
        recipient_email = TEST_EMAIL if TEST_MODE else client_data.get('email')
        if not recipient_email:
            logging.warning(f"Email not sent. No email address for client {client_data['client_name']}.")
            return

        dynamic_template_data = {
            'client_name': client_data['client_name'],
            'cashback': client_data['cashback'],
            'final_tier': client_data['final_tier'],
            'purchase_details': client_data['purchase_details'],
            'daily_checkin_total': client_data['daily_checkin_total'],
            'quarter_spend': client_data['quarter_spend'],
            'lifetime_spend': client_data['lifetime_spend']
        }

        message = Mail(from_email=from_email, to_emails=recipient_email)
        message.template_id = template_id
        message.dynamic_template_data = dynamic_template_data

        # Set unsubscribe group information
        asm = Asm(group_id=23817, groups_to_display=[23816, 23831, 23817])
        message.asm = asm

        response = sg_client.send(message)
        if response.status_code not in range(200, 300):
            raise Exception(f"Failed to send email: {response.status_code}")

        logging.info(f"Email successfully sent to {recipient_email}")

    except Exception as e:
        logging.error(f"Error sending email to {recipient_email}: {e}")
        if retry_count < 3:
            time.sleep(2 ** retry_count)
            send_email(client_data, retry_count + 1)

def main():
    clients_data = fetch_and_process_data()
    email_count = 0

    for client_id, data in clients_data.items():
        if TEST_MODE and email_count >= EMAIL_SEND_LIMIT:
            logging.info("Email send limit reached in test mode.")
            break

        send_email(data)
        email_count += 1

    logging.info("Email sending process completed.")

if __name__ == "__main__":
    logging.info("Starting the script...")
    main()
