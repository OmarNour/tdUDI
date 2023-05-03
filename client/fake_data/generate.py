from server.functions import *

fake = Faker()

import random
from datetime import datetime
from faker import Faker
import teradatasql

fake = Faker()


def djezzy_fake_data(num_records, teradata_conn_info):
    load_id = fake.uuid4()
    with teradatasql.connect(**teradata_conn_info) as con:
        with con.cursor() as cur:
            ref_key = 1

            for _ in range(num_records):
                # DBSS_CRM_TRANSACTIONSTRANSACTION
                transaction = {
                    "CDC_CODE": random.randint(-128, 127),  # Adjusted range for BYTEINT
                    "CUSTOMER_INFORMATION": fake.text(max_nb_chars=32),
                    "DEALERS_CODE": fake.text(max_nb_chars=32),
                    "FILE_ARRIVING_DATE": fake.date(pattern="%Y-%m-%d"),
                    "ID": fake.random_int(min=1, max=2147483647),
                    "INVOICE_INFORMATION": fake.text(max_nb_chars=200),
                    "INVOICE_TYPE": random.randint(0, 10),  # Adjusted range for testing
                    "LAST_MODIFIED": fake.date_time_this_decade(),
                    "SALESMEN_CODE": fake.text(max_nb_chars=32),
                    "TRANSACTION_DATE": fake.date_time_this_decade(),
                    "TRANSACTION_IDENTIFIER": fake.text(max_nb_chars=32),
                    "TRANSACTION_TYPE": random.randint(0, 10),  # Adjusted range for testing
                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": fake.random_int(min=1, max=100),  # Adjusted range for testing
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                transaction_columns = ", ".join(transaction.keys())
                transaction_values = ", ".join(["?"] * len(transaction))
                cur.execute(f"INSERT INTO GDEV1T_STG.DBSS_CRM_TRANSACTIONSTRANSACTION ({transaction_columns}) VALUES ({transaction_values})", list(transaction.values()))
                print(f"{cur.rowcount}, rows inserted into DBSS_CRM_TRANSACTIONSTRANSACTION")
                # JSON_SALES_STG
                json_sales = {
                    # Add your columns and data generation code here
                    "ID": fake.random_int(min=1, max=2147483647),
                    "TRANSACTION_ID": transaction["ID"],

                    "AMOUNT_BEFORE_STAMP_DUTY": round(random.uniform(1, 1000), 2),
                    "AMOUNT_WITHOUT_VAT": round(random.uniform(1, 1000), 2),
                    "CDC_CODE": fake.random_int(min=1, max=9),
                    "CONFIRMATION_CODE": fake.uuid4(),
                    "DEALERS_CODE": fake.bothify(text="??#####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                    "FILE_ARRIVING_DATE": fake.date_this_year(before_today=True, after_today=False),
                    "INVOICE_DATE_TIME": fake.date_time_this_year(before_now=True, after_now=False, tzinfo=None).strftime('%Y-%m-%d %H:%M:%S'),
                    "INVOICE_INFORMATION": fake.text(max_nb_chars=100),
                    "INVOICE_NUMBER": fake.bothify(text="INV-?#####"),
                    "LAST_MODIFIED": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "SALESMAN_CODE": fake.bothify(text="SLSM-?#####"),
                    "STAMP_DUTY": round(random.uniform(0, 50), 2),
                    "TOTAL_AMOUNT": round(random.uniform(1, 1000), 2),
                    "VAT": round(random.uniform(0, 100), 2),
                    "PROVINCE": fake.state(),
                    "SHOP_ADDRESS": fake.street_address(),
                    "PAYMENT_MODES": fake.random_element(elements=("CASH", "CARD", "ONLINE")),
                    "NAME": fake.name(),
                    "TITL": fake.job(),
                    "MSISDN": fake.msisdn(),
                    "FISCAL_CODE": fake.bothify(text="??######?#", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                    "NOTES": fake.text(max_nb_chars=100),
                    "INVOICE_TEMPLATE": fake.slug(),
                    "SHOP_NAME": fake.company(),
                    "PD_VALUE_ADDED_TAX": fake.random_element(elements=("STANDARD", "REDUCED", "EXEMPT")),
                    "PD_STAMP_DUTY": str(round(random.uniform(0, 50), 2)),
                    "PD_TOTAL_AMOUNT_TO_BE_PAID": str(round(random.uniform(1, 1000), 2)),
                    "PD_SETTLEMENT_DISCOUNT": str(round(random.uniform(0, 100), 2)),
                    "PD_TOTAL_PRICE_INCLUDING_TAX": str(round(random.uniform(1, 1000), 2)),
                    "PD_TOTAL_AMOUNT_BEFORE_TAX": str(round(random.uniform(1, 1000), 2)),
                    "PD_TOTAL_AMOUNT_WHEN_PAYMENT_IN_CASH": str(round(random.uniform(1, 1000), 2)),
                    "ICC": fake.bothify(text="ICC-?#####"),
                    "SHOP_ID": fake.random_int(min=1, max=999999),
                    "POSTAL_CODE": fake.zipcode(),
                    "ADDRESS": fake.street_address(),
                    "SALESMEN_CODE": fake.bothify(text="SLSM-?#####"),
                    "TITLE_NAME": fake.prefix(),
                    "ORDER_ID": fake.bothify(text="ORD-?#####"),
                    "CITY": fake.city(),
                    "SEQUENCE_NUMBER": fake.bothify(text="SEQ-?#####"),
                    "AMOUNT_BEFORE_TAX": round(random.uniform(1, 1000), 2),
                    "UNIT_PRICE_WITHOUT_VAT": round(random.uniform(1, 1000), 2),
                    "PAYNOW": round(random.uniform(1, 1000), 2),
                    "DESCRIPTION": fake.text(max_nb_chars=100),
                    "PRODUCT_CODE": fake.bothify(text="PROD-?#####"),
                    "QUANTITY": fake.random_int(min=1, max=100),
                    "DISCOUNT": fake.random_int(min=0, max=100),
                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": fake.random_int(min=1, max=100),  # Adjusted range for testing
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                json_sales_columns = ", ".join(json_sales.keys())
                json_sales_values = ", ".join(["?"] * len(json_sales))
                cur.execute(f"INSERT INTO GDEV1T_STG.JSON_SALES_STG ({json_sales_columns}) VALUES ({json_sales_values})", list(json_sales.values()))
                print(f"{cur.rowcount}, rows inserted into JSON_SALES_STG")
                # DBSS_CRM_TRANSACTIONSPAYMENT
                transactions_payment = {
                    "ID": fake.random_int(),
                    "TRANSACTION_ID": transaction["ID"],
                    "AMOUNT": round(random.uniform(1, 1000), 2),
                    "PAYMENT_METHOD": fake.random_element(elements=("Cash", "Credit Card", "Debit Card", "E-Wallet")),
                    "PAYMENT_DATE": fake.date_between(start_date="-1y", end_date="today"),
                    "STATUS": fake.random_element(elements=("Paid", "Pending", "Refunded", "Cancelled")),
                    "CONFIRMATION_CODE": fake.uuid4(),
                    "CURRENCY": fake.currency_code(),
                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": fake.random_int(min=1, max=100),  # Adjusted range for testing
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                transactions_payment_columns = ", ".join(transactions_payment.keys())
                transactions_payment_values = ", ".join(["?"] * len(transactions_payment))
                # cur.execute(f"INSERT INTO GDEV1T_STG.DBSS_CRM_TRANSACTIONSPAYMENT ({transactions_payment_columns}) VALUES ({transactions_payment_values})", list(transactions_payment.values()))
                # print(f"{cur.rowcount}, rows inserted into DBSS_CRM_TRANSACTIONSPAYMENT")
                ref_key += 1
            con.commit()


def aca_fake_data():
    # Connect to Teradata
    ip = "localhost"
    user = "power_user"
    password = "power_user"
    conn = teradatasql.connect(host=ip, user=user, password=password)
    cursor = conn.cursor()
    fake_data_cso(cursor, row_count=100)
    conn.close()


def fake_data_cso(cursor, row_count):
    load_id = fake.uuid4()
    batch_id = random.randint(1, 5)
    # Generate fake data and insert into table
    for i in range(row_count):
        cso_number = random.randint(100000000, 999999999)

        ###### technical columns    ######
        ref_key = random.randint(100000000, 999999999)
        modification_type = random.choice(["I", "U", "D"])
        is_transferred = random.randint(0, 1)
        data_extraction_date = fake.date_time_between(start_date="-1y", end_date="now")
        ins_dttm = fake.date_time_between(start_date="-1y", end_date="now")
        upd_dttm = fake.date_time_between(start_date=ins_dttm, end_date="now")
        delivered_date = fake.date_time_between(start_date="-1y", end_date="now")

        # CSO_ADDRESS data
        address = fake.address()
        address_date = fake.date_time_between(start_date="-10y", end_date="now")
        police_station_id = random.randint(100, 999)
        governorate_id = random.randint(1, 20)

        address1 = fake.address()
        address_date1 = fake.date_time_between(start_date="-10y", end_date="now")
        police_station_id1 = random.randint(100, 999)
        governorate_id1 = random.randint(1, 20)

        # CSO_NEW_PERSON data
        national_id = ''.join(str(random.randint(0, 9)) for _ in range(14))
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=100).strftime('%y/%m/%d')
        first_name = fake.first_name()
        father_name = fake.last_name()
        mother_first_name = fake.first_name()
        mother_second_name = fake.last_name()
        mother_third_name = fake.first_name()
        mother_cso_number = fake.random_int(min=1, max=9999999999)
        father_cso_number = fake.random_int(min=1, max=9999999999)
        gender_id = random.choice([1, 2])  # 1: Male, 2: Female
        religion_id = random.randint(1, 3)

        insert_query = f"INSERT INTO STG_ONLINE.CSO_NEW_PERSON  " \
                       f"(CSO_NUMBER, DELIVERED_DATE, NATIONAL_ID, BIRTH_DATE, FIRST_NAME, FATHER_NAME, MOTHER_FIRST_NAME, MOTHER_SECOND_NAME, MOTHER_THIRD_NAME, MOTHER_CSO_NUMBER, FATHER_CSO_NUMBER, GENDER_ID, RELIGION_ID, REF_KEY, LOAD_ID, BATCH_ID, MODIFICATION_TYPE, IS_TRANSFERRED, DATA_EXTRACTION_DATE, INS_DTTM, UPD_DTTM)  " \
                       f"VALUES({cso_number}, '{delivered_date}', {national_id}, '{birth_date}', '{first_name}', '{father_name}', '{mother_first_name}', '{mother_second_name}', '{mother_third_name}', {mother_cso_number}, {father_cso_number}, {gender_id}, {religion_id}, {ref_key}, '{load_id}', {batch_id}, '{modification_type}', {is_transferred}, '{data_extraction_date}', '{ins_dttm}', '{upd_dttm}') ;"
        cursor.execute(insert_query)

        insert_query = f"INSERT INTO STG_ONLINE.CSO_ADDRESS (CSO_NUMBER, DELIVERED_DATE, ADDRESS, ADDRESS_DATE, POLICE_STATION_ID, GOVERNORATE_ID, REF_KEY, LOAD_ID, BATCH_ID, MODIFICATION_TYPE, IS_TRANSFERRED, DATA_EXTRACTION_DATE, INS_DTTM, UPD_DTTM) " \
                       f"VALUES ({cso_number}, '{delivered_date}', '{address}', '{address_date}', {police_station_id}, {governorate_id}, {ref_key}, '{load_id}', {batch_id}, '{modification_type}', {is_transferred}, '{data_extraction_date}', '{ins_dttm}', '{upd_dttm}')"
        cursor.execute(insert_query)

        insert_query = f"INSERT INTO STG_ONLINE.CSO_ADDRESS (CSO_NUMBER, DELIVERED_DATE, ADDRESS, ADDRESS_DATE, POLICE_STATION_ID, GOVERNORATE_ID, REF_KEY, LOAD_ID, BATCH_ID, MODIFICATION_TYPE, IS_TRANSFERRED, DATA_EXTRACTION_DATE, INS_DTTM, UPD_DTTM) " \
                       f"VALUES ({cso_number}, '{delivered_date}', '{address1}', '{address_date1}', {police_station_id1}, {governorate_id1}, {ref_key}, '{load_id}', {batch_id}, '{modification_type}', {is_transferred}, '{data_extraction_date}', '{ins_dttm}', '{upd_dttm}')"
        cursor.execute(insert_query)

    print(f"{row_count} rows of fake data generated and inserted into STG_ONLINE.CSO_ADDRESS table.")
    print(f"{row_count} rows of fake data generated and inserted into STG_ONLINE.CSO_NEW_PERSON table.")

    cdc_audit = f"""INSERT INTO GDEV1_ETL.CDC_AUDIT  
                    (SOURCE_NAME, LOAD_ID, BATCH_ID, TABLE_NAME, PROCESSED, RECORDS_COUNT)  
                    VALUES('CSO','{load_id}',{batch_id},'CSO_ADDRESS',0,{row_count}) ;"""
    cursor.execute(cdc_audit)

    cdc_audit = f"""INSERT INTO GDEV1_ETL.CDC_AUDIT  
                        (SOURCE_NAME, LOAD_ID, BATCH_ID, TABLE_NAME, PROCESSED, RECORDS_COUNT)  
                        VALUES('CSO','{load_id}',{batch_id},'CSO_NEW_PERSON',0,{row_count}) ;"""
    cursor.execute(cdc_audit)


if __name__ == '__main__':
    # aca_fake_data()
    # Example usage
    teradata_conn_info = {
        "host": "localhost",
        "user": "power_user",
        "password": "power_user",
        "database": ""
    }
    djezzy_fake_data(10, teradata_conn_info)
