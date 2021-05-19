import boto3
import csv
import urllib
import psycopg2

endpoint = 'kgalife.ccgci6xjexf4.ap-southeast-1.rds.amazonaws.com'
user = 'postgres'
password = '12345678'
db_name = 'postgres'


def lambda_handler(event, context):
    try:
        connection = psycopg2.connect(user=user, password=password, host=endpoint, port="5432", database=db_name)
        cursor = connection.cursor()

        bucket = event['Records'][0]['s3']['bucket']['name']
        file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        s3 = boto3.client('s3')
        csvfile = s3.get_object(Bucket=bucket, Key=file_key)
        csvcontent = csvfile['Body'].read().decode('utf-8').splitlines()

        lines = csv.reader(csvcontent)
        headers = next(lines)
        for line in lines:
            try:
                insert_query = "INSERT INTO landing.policy_termination ( policy, surname, initials, id_number, cell_number, premium, pay_method, broker, agent_name, agent_code, date_captured, inception_date, first_billing_date, date_cancelled, reason_for_cancellation, last_payment_date, clawback_amount, date_clawed_back, claw_back_pending, status, number_of_payments, consecutive_arrears, bank, account_type, account_number, deduction_day, client) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (line[0] ,line[1] ,line[2] ,line[3] ,line[4] ,line[5] ,line[6] ,line[7] ,line[8] ,line[9] ,line[10] ,line[11] ,line[12] ,line[13] ,line[14] ,line[15] ,line[16] ,line[17] ,line[18] ,line[19] ,line[20] ,line[21] ,line[22] ,line[23] ,line[24] ,line[25] ,line[26])
                print(insert_query)
                cursor.execute(insert_query)
                connection.commit()
            except:
                continue
        cursor.close()
        connection.close()
    except Exception as err:
        print(err)

