import random
from datetime import datetime
from faker import Faker
import teradata

fake = Faker()

# Teradata connection details
udaExec = teradata.UdaExec(appName="FakeDataGenerator", version="1.0", logConsole=False)
session = udaExec.connect(method="odbc", system="your_system", username="your_username", password="your_password")


def generate_and_insert_fake_data(num_records):
    for _ in range(num_records):
        # Generate data for GDEV1T_STG.DBSS_CRM_TRANSACTIONSTRANSACTION
        transaction_data = {
            "CDC_CODE": fake.random_int(min=1, max=255),
            "CUSTOMER_INFORMATION": fake.text(max_nb_chars=32),
            "DEALERS_CODE": fake.text(max_nb_chars=32),
            "FILE_ARRIVING_DATE": fake.date_this_year(before_today=True, after_today=False).strftime('%y/%m/%d'),
            "ID": fake.random_int(min=1, max=999999),
            "INVOICE_INFORMATION": fake.text(max_nb_chars=200),
            "INVOICE_TYPE": fake.random_int(min=1, max=9999),
            "LAST_MODIFIED": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "SALESMEN_CODE": fake.text(max_nb_chars=32),
            "TRANSACTION_DATE": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "TRANSACTION_IDENTIFIER": fake.text(max_nb_chars=32),
            "TRANSACTION_TYPE": fake.random_int(min=1, max=9999),
            "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
            "LOAD_ID": fake.uuid4(),
            "BATCH_ID": fake.random_int(min=1, max=999999),
            "REF_KEY": fake.unique.random_int(min=1, max=99999999999),
            "INS_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "UPD_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        }

        # Insert data into GDEV1T_STG.DBSS_CRM_TRANSACTIONSTRANSACTION
        transaction_columns = ', '.join(transaction_data.keys())
        transaction_values = ', '.join(['?'] * len(transaction_data))
        session.execute(f"INSERT INTO GDEV1T_STG.DBSS_CRM_TRANSACTIONSTRANSACTION ({transaction_columns}) VALUES ({transaction_values})", tuple(transaction_data.values()))

        # Generate data for GDEV1T_STG.JSON_SALES_STG
        json_sales_data = {
            # Add all columns from the JSON_SALES_STG table schema, similar to transaction_data
            # ...
            "TRANSACTION_ID": transaction_data["ID"],
            "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
            "LOAD_ID": fake.uuid4(),
            "BATCH_ID": fake.random_int(min=1, max=999999),
            "REF_KEY": fake.unique.random_int(min=1, max=99999999999),
            "INS_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "UPD_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        }

        # Insert data into GDEV1T_STG.JSON_SALES_STG
        json_sales_columns = ', '.join(json_sales_data.keys())
        json_sales_values = ', '.join(['?'] * len(json_sales_data))
        session.execute(f"INSERT INTO GDEV1T_STG.JSON_SALES_STG ({json_sales_columns}) VALUES ({json_sales_values})", tuple(json_sales_data.values()))

        # Generate data for GDEV1T_STG.DBSS_CRM_TRANSACTIONSPAYMENT
        transaction_payment_data = {
            # Add all columns from the DBSS_CRM_TRANSACTIONSPAYMENT table schema, similar to transaction_data
            # ...
            "TRANSACTION_ID": transaction_data["ID"],
            "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
            "LOAD_ID": fake.uuid4(),
            "BATCH_ID": fake.random_int(min=1, max=999999),
            "REF_KEY": fake.unique.random_int(min=1, max=99999999999),
            "INS_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "UPD_DTTM": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        }

        # Insert data into GDEV1T_STG.DBSS_CRM_TRANSACTIONSPAYMENT
        transaction_payment_columns = ', '.join(transaction_payment_data.keys())