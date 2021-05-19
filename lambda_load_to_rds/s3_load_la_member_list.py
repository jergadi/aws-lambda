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
                insert_query = "INSERT INTO landing.member_list (type ,policy_number ,title ,first_name ,last_name ,id_number ,relation ,gender ,birth_date ,age ,date_captured ,inception_date ,waiting_period ,policy_code ,policy_description ,premium ,uw_premium ,cover ,under_writer ,status ,broker ,branch ,agent ,cell_number ,payment_type ,first_fiscal_period ,last_fiscal_period ,first_transaction_date ,last_transaction_date ,additional_products ,current_age ,pay_at_number ,work_permit ,premium_escalation_prcnt ,cover_escalation_prcnt ,beneficiary_full_name_1 ,beneficiary_id_number_1 ,beneficiary_relation_1 ,beneficiary_percentage_1 ,beneficiary_full_name_2 ,beneficiary_id_number_2 ,beneficiary_relation_2 ,beneficiary_percentage_2 ,beneficiary_full_name_3 ,beneficiary_id_number_3 ,beneficiary_relation_3 ,beneficiary_percentage_3 ,beneficiary_full_name_4 ,beneficiary_id_number_4 ,beneficiary_relation_4 ,beneficiary_percentage_4 ,beneficiary_full_name_5 ,beneficiary_id_number_5 ,beneficiary_relation_5 ,beneficiary_percentage_5 ) VALUES ( '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s');" % (line[0] ,line[1] ,line[2] ,line[3] ,line[4] ,line[5] ,line[6] ,line[7] ,line[8] ,line[9] ,line[10] ,line[11] ,line[12] ,line[13] ,line[14] ,line[15] ,line[16] ,line[17] ,line[18] ,line[19] ,line[20] ,line[21] ,line[22] ,line[23] ,line[24] ,line[25] ,line[26] ,line[27] ,line[28] ,line[29] ,line[30] ,line[31] ,line[32] ,line[33] ,line[34] ,line[35] ,line[36] ,line[37] ,line[38] ,line[39] ,line[40] ,line[41] ,line[42] ,line[43] ,line[44] ,line[45] ,line[46] ,line[47] ,line[48] ,line[49] ,line[50] ,line[51] ,line[52] ,line[53] ,line[54])
                print(insert_query)
                cursor.execute(insert_query)
                connection.commit()
            except:
                continue
        cursor.close()
        connection.close()

    except Exception as err:
        print(err)

