# Spike cost alert in GCP 
This Cloud Functions Python script sents a Cloud Monitoring alert vía email to desingated users. 

It's triggered by peaking in costs. You can configure it to alert when a service or a project's cost is above certain threshold in day to day analysis. 

# How to deploy 

Creat cloud function and upload the code to the source 

Entrypoint: peak_daily_cost_alert
Runtime : Python 3.11 