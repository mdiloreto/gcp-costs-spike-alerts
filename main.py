import base64
import os 
import csv
import io
from datetime import datetime, timezone, timedelta
#import urllib.parse
from google.cloud import bigquery
import google.cloud.logging
import json
import logging


# ENV Vars
# BILLING_ORG = os.environ.get("BILLING_ORG")
# BILLING_ID = os.environ.get("BILLING_ID")
BQ_TABLE = os.environ.get("BQ_TABLE")
BQ_TABLE = "vog_arg_prod_billing_export_daily.gcp_billing_export_v1_011241_4C1B85_0613CB"
#print(BQ_TABLE)
PROJECT_ID = os.environ.get("PROJECT_ID")
PROJECT_ID = 'vog-arg-prod-billingexport'

# Set default thresholds
AMOUNT = 250
AMOUNT_CHANGED = 500
PERCENTAGE = 1.1

def peak_daily_cost_alert(event, context):
#def peak_daily_cost_alert():
    #print("Inicio script.....")
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    data = json.loads(pubsub_message)
    print(data["AMOUNT"])
    print(data["AMOUNT_CHANGED"])
    print(data["PERCENTAGE"])

    # Set thresholds
    global AMOUNT
    global AMOUNT_CHANGED
    global PERCENTAGE

    if "AMOUNT" in data:
        AMOUNT = float(data["AMOUNT"])
    if "AMOUNT_CHANGED" in data:
        AMOUNT_CHANGED = float(data["AMOUNT_CHANGED"])
    if "PERCENTAGE" in data:
        PERCENTAGE = float(data["PERCENTAGE"])

    print("Thresholds: AMOUNT=",AMOUNT, " AMOUNT_CHANGED=", AMOUNT_CHANGED, " PERCENTAGE=",PERCENTAGE)

    daily_costs = get_daily_costs()
    flagged_costs = parse_cost_changes(daily_costs)

    #GCP logging. Para imprimir en Cloud Loggin
    client = google.cloud.logging.Client()
    client.setup_logging()

    if flagged_costs:
        now = datetime.now(timezone.utc)
        prev_utc = now - timedelta(2)
        curr_utc = now - timedelta(1)

        #CSV output
        output = io.StringIO()
        writer = csv.writer(output)
        #CSV headers
        fields = ["project", "SKU", "Service", "Previous Cost ("+ prev_utc.strftime('%Y-%m-%d') + ")", "Latest Cost ("+ curr_utc.strftime('%Y-%m-%d')+ ")"]
        writer.writerow(fields)
        csv_string = output.getvalue()

        for cost in flagged_costs:
            # Add Additional Integrations Here 
            #alerts += getData(cost)
            #Agrego filas al CSV
            writer.writerow(getData(cost))

        #Obtengo CSV generado 
        csv_string = output.getvalue()

        #Escribo CSV en Cloud Logging
        logging.warning(csv_string)
    else:
        logging.info("It´s all OK")

# Query to get Daily Costs
def get_daily_costs():
    client = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc)
    prev_utc = now - timedelta(2)
    curr_utc = now - timedelta(1)
    
    QUERY = (
        f"""SELECT
        project.name as project,
        sku.id as sku_id,
        sku.description as sku_def,
        service.id as service_id,
        service.description as service_def,
        SUM(CASE WHEN EXTRACT(DAY FROM usage_start_time) = {prev_utc.day} THEN cost ELSE 0 END) AS prev_day,
        SUM(CASE WHEN EXTRACT(DAY FROM usage_start_time) = {curr_utc.day} THEN cost ELSE 0 END) AS curr_day,
        FROM `{BQ_TABLE}`
        WHERE DATE_TRUNC(usage_start_time, DAY) = "{prev_utc.strftime('%Y-%m-%d')}" or DATE_TRUNC(usage_start_time, DAY) = "{curr_utc.strftime('%Y-%m-%d')}"
        GROUP BY 1,2,3,4,5
        ORDER BY 1;"""

		## >>> Query for projects <<< 
  
        # f"""SELECT
        # project.name as project,
        # SUM(CASE WHEN EXTRACT(DAY FROM usage_start_time) = {prev_utc.day} THEN cost ELSE 0 END) AS prev_day,
        # SUM(CASE WHEN EXTRACT(DAY FROM usage_start_time) = {curr_utc.day} THEN cost ELSE 0 END) AS curr_day,
        # FROM `{BQ_TABLE}`
        # WHERE DATE_TRUNC(usage_start_time, DAY) = "{prev_utc.strftime('%Y-%m-%d')}" or DATE_TRUNC(usage_start_time, DAY) = "{curr_utc.strftime('%Y-%m-%d')}"
        # GROUP BY 1
        # ORDER BY 1;"""
    )

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()
    return rows

# Extract Large Spikes from Costs
def parse_cost_changes(rows):
    flagged_items = []
    for item in rows:
        i1 = float(item["prev_day"])
        i2 = float(item["curr_day"])
        if (i1 > AMOUNT or i2 > AMOUNT) and i1 != 0 and i2 != 0 and ((i2 /i1) >= PERCENTAGE) or abs(i2-i1) >= AMOUNT_CHANGED: # old percentaje condition that includes decrease in cost -->((i2 /i1) >= PERCENTAGE or (i1 /i2) >= PERCENTAGE)
            flagged_items.append(item)
    return flagged_items

def getData(item):
    #encoded_sku = urllib.parse.quote_plus(f"services/{item['service_id']}/skus/{item['sku_id']}")
    #billing_link = f"https://console.cloud.google.com/billing/{BILLING_ID}/reports;projects={item['project']};skus={encoded_sku}?organizationId={BILLING_ORG}&orgonly=true"
    
    now = datetime.now(timezone.utc)
    prev_utc = now -timedelta(2)
    curr_utc = now - timedelta(1)
    #print("Warning_of_costpeaks in the project: ", item['project'], " | SKU: ", item['sku_def'], " | Service: ", item['service_def'], " | Previous Cost (", prev_utc.strftime('%Y-%m-%d'), "): ", "{:.2f}".format(item['prev_day']), " | Latest Cost (", curr_utc.strftime('%Y-%m-%d'), "): ", "{:.2f}".format(item['curr_day']))
    #return ("Warning_of_costpeaks in the project: " + item['project']+ " | SKU: "+ item['sku_def']+ " | Service: "+ item['service_def']+ " | Previous Cost ("+ prev_utc.strftime('%Y-%m-%d')+ "): "+ "{:.2f}".format(item['prev_day'])+ " | Latest Cost ("+ curr_utc.strftime('%Y-%m-%d')+ "): "+ "{:.2f}".format(item['curr_day']))
    return [item['project'], item['sku_def'], item['service_def'], "{:.2f}".format(item['prev_day']), "{:.2f}".format(item['curr_day'])]

#peak_daily_cost_alert()