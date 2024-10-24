import pandas as pd
import logging
from datetime import datetime, timedelta
from emailing import email_generator
from Db_Request import establish_connection
from Failedmail import Failed_mail
# Get the current datetime
current_datetime = datetime.now()

# Extract date and time separately
current_date = current_datetime.strftime('%Y-%m-%d')
current_time = current_datetime.strftime('%H:%M')
yesterday_date = (current_datetime - timedelta(days=1)).strftime('%Y-%m-%d')

# Define SQL queries based on time range
if '13:59'>current_time >= '10:00':
    query1 = f'''SELECT * FROM (
                SELECT create_date
                FROM tb_tvs_common_api_leads 
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                
                UNION ALL
                
                SELECT create_date
                FROM tb_tvscredit_common_api_leads
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                UNION ALL
                SELECT create_date
                FROM tvs_all_campaign_leads 
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                ) AS combined_data;
                '''
    query2 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    ) AS combined_data;
                '''
    query3 = f'''SELECT * FROM (
                    SELECT create_date FROM tb_tvs_common_api_leads WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                    UNION ALL
                    SELECT create_date FROM tb_tvscredit_common_api_leads WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                    UNION ALL
                    SELECT create_date FROM tvs_all_campaign_leads WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000'
                    ) AS combined_data;
                '''
    query4 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{yesterday_date} 18:00:00.000' AND '{current_date} 10:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    ) AS combined_data;
                '''
    t1 = f'{yesterday_date} 06:00 PM'
    t2 = f'{current_date} 10:00 AM'
elif '14:00' < current_time <= '18:00':
    query1 = f'''SELECT * FROM (SELECT create_date FROM tb_tvs_common_api_leads WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                UNION ALL
                SELECT create_date FROM tb_tvscredit_common_api_leads WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                UNION ALL
                SELECT create_date FROM tvs_all_campaign_leads WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                ) AS combined_data;
                '''
    query2 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    ) AS combined_data;
                '''
    query3 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                    UNION ALL
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                    UNION ALL
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000'
                    ) AS combined_data;
                '''
    query4 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{current_date} 10:00:00.000' AND '{current_date} 14:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    ) AS combined_data;
                '''
    t1, t2 = f'{current_date} 10:00 AM', f'{current_date} 02:00 PM'
elif '18:00' < current_time <= '10:00':
    query1 = f'''SELECT * FROM (
                SELECT create_date
                FROM tb_tvs_common_api_leads 
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                UNION ALL
                SELECT create_date
                FROM tb_tvscredit_common_api_leads
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                UNION ALL
                SELECT create_date
                FROM tvs_all_campaign_leads 
                WHERE model_id IN (19) 
                AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                ) AS combined_data;
                '''
    query2 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (19)
                    ) AS combined_data;
                '''
    query3 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                    UNION ALL
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                    UNION ALL
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE model_id IN (1,15,21,25,26,27) 
                    AND create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000'
                    ) AS combined_data;
                '''
    query4 = f'''SELECT * FROM (
                    SELECT create_date
                    FROM tb_tvs_common_api_leads 
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    
                    UNION ALL
                    
                    SELECT create_date
                    FROM tb_tvscredit_common_api_leads
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    UNION ALL
                    
                    SELECT create_date
                    FROM tvs_all_campaign_leads 
                    WHERE create_date BETWEEN '{current_date} 14:00:00.000' AND '{current_date} 18:00:00.000' AND ems_server_response LIKE '%ERROR%' AND model_id IN (1,15,21,25,26,27)
                    ) AS combined_data;
                '''
    t1, t2 = f'{current_date} 02:00 PM', f'{current_date} 06:00PM'
    print(f"time rage:{t1} to {t2}")
else:
    print("Invalid time range")
    logging.error("Invalid time range")

# Executing the SQL queries and generating the report
try:
    rows_query1 = establish_connection(query1)
    Ronin_total = len(rows_query1)
    print(f"Ronin_total: {Ronin_total}")
    logging.info(f"Ronin_total: {Ronin_total}")

    rows_query2 = establish_connection(query2)
    Ronin_Failed = len(rows_query2)
    print(f"Ronin_Failed: {Ronin_Failed}")
    logging.info(f"Ronin_Failed: {Ronin_Failed}")

    rows_query3 = establish_connection(query3)
    ApacheTotal = len(rows_query3)
    print(f"ApacheTotal: {ApacheTotal}")
    logging.info(f"ApacheTotal: {ApacheTotal}")

    rows_query4 = establish_connection(query4)
    ApacheFailed = len(rows_query4)
    print(f"ApacheFailed: {ApacheFailed}")
    logging.info(f"ApacheFailed: {ApacheFailed}")

    # Prepare dataframes for the email
    data2 = {'Ronin Leads': ['Total Leads', 'Failed Leads'], 'Counts': [Ronin_total, Ronin_Failed]}
    data = {'Apache Leads': ['Total Leads', 'Failed Leads'], 'Counts': [ApacheTotal, ApacheFailed]}

    df = pd.DataFrame(data)
    df1 = pd.DataFrame(data2)

    # Send the email
    print(f"time rage:{t1} to {t2}")
    email_generator(df, df1, t1, t2)
except Exception as e:
    print(f"An error occurred: {e}")
    logging.error(f"An error occurred: {e}")
    Failed_mail(t1,t2,e)
