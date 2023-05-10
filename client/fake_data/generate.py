from server.functions import *

fake = Faker()

import random
from datetime import datetime
from faker import Faker
import teradatasql

fake = Faker()


def djezzy_fake_data(num_records, start_from=0, teradata_conn_info=None):
    """

    :param num_records:
    :param start_from:
    :param teradata_conn_info:
    :return:


    CREATE multiset TABLE STG_ONLINE.DBSS_CRM_TRANSACTIONSTRANSACTION
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         CDC_CODE  BYTEINT
        ,CUSTOMER_INFORMATION  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,DEALERS_CODE  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,FILE_ARRIVING_DATE  DATE
        ,ID  INTEGER   not null
        ,INVOICE_INFORMATION  VARCHAR(200) CHARACTER SET latin not CASESPECIFIC
        ,INVOICE_TYPE  SMALLINT
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,SALESMEN_CODE  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,TRANSACTION_DATE  TIMESTAMP(0)
        ,TRANSACTION_IDENTIFIER  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,TRANSACTION_TYPE  SMALLINT
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );


    CREATE multiset TABLE STG_ONLINE.JSON_SALES_STG
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         AMOUNT_BEFORE_STAMP_DUTY  DECIMAL(16,2)
        ,AMOUNT_WITHOUT_VAT  DECIMAL(16,2)
        ,CDC_CODE  BYTEINT
        ,CONFIRMATION_CODE  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,DEALERS_CODE  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,FILE_ARRIVING_DATE  DATE
        ,ID  INTEGER   not null
        ,INVOICE_DATE_TIME  TIMESTAMP(0)
        ,INVOICE_INFORMATION  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,INVOICE_NUMBER  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,SALESMAN_CODE  VARCHAR(32) CHARACTER SET latin not CASESPECIFIC
        ,STAMP_DUTY  DECIMAL(16,2)
        ,TOTAL_AMOUNT  DECIMAL(16,2)
        ,TRANSACTION_ID  INTEGER   --> = DBSS_CRM_TRANSACTIONSTRANSACTION.id
        ,VAT  DECIMAL(16,2)
        ,PROVINCE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,SHOP_ADDRESS  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PAYMENT_MODES  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,NAME  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,TITL  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,MSISDN  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,FISCAL_CODE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,NOTES  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,INVOICE_TEMPLATE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,SHOP_NAME  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_VALUE_ADDED_TAX  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_STAMP_DUTY  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_TOTAL_AMOUNT_TO_BE_PAID  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_SETTLEMENT_DISCOUNT  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_TOTAL_PRICE_INCLUDING_TAX  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_TOTAL_AMOUNT_BEFORE_TAX  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PD_TOTAL_AMOUNT_WHEN_PAYMENT_IN_CASH  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,ICC  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,SHOP_ID  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,POSTAL_CODE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,ADDRESS  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,SALESMEN_CODE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,TITLE_NAME  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,ORDER_ID  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,CITY  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,SEQUENCE_NUMBER  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,AMOUNT_BEFORE_TAX  DECIMAL(16,2)
        ,UNIT_PRICE_WITHOUT_VAT  DECIMAL(16,2)
        ,PAYNOW  DECIMAL(16,2)
        ,DESCRIPTION  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,PRODUCT_CODE  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,QUANTITY  INTEGER
        ,DISCOUNT  INTEGER
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );

    CREATE multiset TABLE STG_ONLINE.DBSS_CRM_TRANSACTIONSPAYMENT
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         BEFORE_VAT_AMOUNT  DECIMAL(32,6)
        ,CDC_CODE  BYTEINT
        ,CHEQUE_NUMBER  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,FILE_ARRIVING_DATE  DATE
        ,ID  INTEGER   not null
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,PAID_AMOUNT  DECIMAL(32,6)
        ,PAYMENT_MODE  SMALLINT
        ,SETTLEMENT_DISCOUNT  DECIMAL(32,6)
        ,TAX_PAYMENT  DECIMAL(32,6)
        ,TOTAL_PAID  DECIMAL(32,6)
        ,TRANSACTION_ID  INTEGER   --> = DBSS_CRM_TRANSACTIONSTRANSACTION.id
        ,VAT  DECIMAL(32,6)
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );


    CREATE multiset TABLE STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         ID  INTEGER   not null
        ,CUSTOMER_ID  INTEGER
        ,CONFIRMATION_CODE  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC --> = JSON_SALES_STG.CONFIRMATION_CODE
        ,CREATED_AT  TIMESTAMP(0)
        ,SALES_INFO_ID  INTEGER
        ,FILE_ARRIVING_DATE  DATE
        ,STATUS  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,DELIVERY_TYPE  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,BRAND  INTEGER
        ,ORDERED_AT  TIMESTAMP(0)
        ,DELIVERY_ADDRESS_ID  INTEGER
        ,OFFER  VARCHAR(1024) CHARACTER SET latin not CASESPECIFIC
        ,CORRECTION_FOR  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,DEVICES_RETURNED_AT  TIMESTAMP(0)
        ,PURCHASE_ORDER_ID  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,CDC_CODE  BYTEINT
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );

    CREATE multiset TABLE STG_ONLINE.DBSS_PC_PRODUCTSITEMVARIANT
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         CART_IMAGE  VARCHAR(100) CHARACTER SET latin not CASESPECIFIC
        ,CODE  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,COLOR_ID  INTEGER
        ,COVER_IMAGE  VARCHAR(100) CHARACTER SET latin not CASESPECIFIC
        ,DISPLAY_ORDER  INTEGER
        ,FILE_ARRIVING_DATE  DATE
        ,ID  INTEGER   not null
        ,ITEM_MODEL_ID  INTEGER
        ,NAME_PRODUCTSITEMVARIANT  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,OUT_OF_STOCK  INTEGER
        ,OUT_OF_STOCK_ACTION  VARCHAR(64) CHARACTER SET latin not CASESPECIFIC
        ,STATUS  INTEGER
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );

    CREATE multiset TABLE STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         ID  INTEGER   not null
        ,CONTRACT_ID  INTEGER   --> = DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT.id
        ,TYPE_ID  INTEGER   --> = DBSS_PC_PRODUCTSITEMVARIANT.id
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,PRICE  DECIMAL(16,6)
        ,ARTICLE_ID  VARCHAR(4096) CHARACTER SET latin not CASESPECIFIC
        ,DATA  VARCHAR(5500) CHARACTER SET latin not CASESPECIFIC
        ,QUANTITY  INTEGER
        ,CDC_CODE  BYTEINT
        ,FILE_ARRIVING_DATE  DATE
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );

    CREATE multiset TABLE STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDSIMCARD
        ,FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        ,CHECKSUM = DEFAULT
        ,DEFAULT MERGEBLOCKRATIO
    (
         ID  INTEGER   not null
        ,PRICE  DECIMAL(16,6)
        ,CONTRACT_ID  INTEGER   -- = DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT.id
        ,LAST_MODIFIED  TIMESTAMP(0)
        ,IMSI  VARCHAR(129) CHARACTER SET latin not CASESPECIFIC
        ,ITEM  VARCHAR(100) CHARACTER SET latin not CASESPECIFIC
        ,ARTICLE_ID  VARCHAR(128) CHARACTER SET latin not CASESPECIFIC
        ,MODIFICATION_TYPE  CHAR(1) CHARACTER SET latin not CASESPECIFIC not null
        ,LOAD_ID  VARCHAR(60) CHARACTER SET latin not CASESPECIFIC not null
        ,BATCH_ID  INT   not null
        ,REF_KEY  BIGINT   not null
        ,INS_DTTM  TIMESTAMP(6)   not null
        ,UPD_DTTM  TIMESTAMP(6)
     )
    unique PRIMARY INDEX ( ID );

    """
    load_id = fake.uuid4()
    batch_id = random.randint(1, 5)
    with teradatasql.connect(**teradata_conn_info) as con:
        with con.cursor() as cur:
            ref_key = 1
            trx_id = 1 + start_from
            deletes = ["delete from STG_ONLINE.DBSS_CRM_TRANSACTIONSTRANSACTION;"
                , "delete from STG_ONLINE.DBSS_CRM_TRANSACTIONSPAYMENT"
                , "delete from STG_ONLINE.JSON_SALES_STG"
                , "delete from STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT"
                , "delete from STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE"
                , "delete from STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDSIMCARD"
                , "delete from STG_ONLINE.DBSS_PC_PRODUCTSITEMVARIANT"
                       ]
            for _ in deletes:
                cur.execute(_)
                con.commit()

            for _ in range(num_records):
                # DBSS_CRM_TRANSACTIONSTRANSACTION
                transaction = {
                    "CDC_CODE": random.randint(-128, 127),  # Adjusted range for BYTEINT
                    "CUSTOMER_INFORMATION": fake.text(max_nb_chars=32),
                    "DEALERS_CODE": fake.text(max_nb_chars=32),
                    "FILE_ARRIVING_DATE": fake.date(pattern="%Y-%m-%d"),
                    "ID": trx_id,
                    "INVOICE_INFORMATION": fake.text(max_nb_chars=200),
                    "INVOICE_TYPE": random.randint(0, 10),  # Adjusted range for testing
                    "LAST_MODIFIED": fake.date_time_this_decade(),
                    "SALESMEN_CODE": fake.text(max_nb_chars=32),
                    "TRANSACTION_DATE": fake.date_time_this_decade(),
                    "TRANSACTION_IDENTIFIER": fake.text(max_nb_chars=32),
                    "TRANSACTION_TYPE": random.randint(0, 10),  # Adjusted range for testing
                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                transaction_columns = ", ".join(transaction.keys())
                transaction_values = ", ".join(["?"] * len(transaction))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_CRM_TRANSACTIONSTRANSACTION ({transaction_columns}) VALUES ({transaction_values})", list(transaction.values()))

                # JSON_SALES_STG
                confirmation_code = fake.uuid4()
                json_sales = {
                    # Add your columns and data generation code here
                    "ID": trx_id + 1,
                    "TRANSACTION_ID": transaction["ID"],

                    "AMOUNT_BEFORE_STAMP_DUTY": round(random.uniform(1, 1000), 2),
                    "AMOUNT_WITHOUT_VAT": round(random.uniform(1, 1000), 2),
                    "CDC_CODE": fake.random_int(min=1, max=9),
                    "CONFIRMATION_CODE": confirmation_code,
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
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                json_sales_columns = ", ".join(json_sales.keys())
                json_sales_values = ", ".join(["?"] * len(json_sales))
                cur.execute(f"INSERT INTO STG_ONLINE.JSON_SALES_STG ({json_sales_columns}) VALUES ({json_sales_values})", list(json_sales.values()))

                # DBSS_CRM_TRANSACTIONSPAYMENT
                transactions_payment = {
                    "ID": trx_id + 2,
                    "TRANSACTION_ID": transaction["ID"],

                    "BEFORE_VAT_AMOUNT": round(random.uniform(1, 1000), 6),
                    "CDC_CODE": fake.random_int(min=1, max=10),
                    "CHEQUE_NUMBER": fake.bothify(text="CHQ-?#####"),
                    "FILE_ARRIVING_DATE": fake.date_this_year(before_today=True, after_today=False).strftime('%y/%m/%d'),
                    "LAST_MODIFIED": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "PAID_AMOUNT": round(random.uniform(1, 1000), 6),
                    "PAYMENT_MODE": fake.random_int(min=1, max=9999),
                    "SETTLEMENT_DISCOUNT": round(random.uniform(0, 100), 6),
                    "TAX_PAYMENT": round(random.uniform(0, 100), 6),
                    "TOTAL_PAID": round(random.uniform(1, 1000), 6),
                    "VAT": round(random.uniform(0, 100), 6),

                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                transactions_payment_columns = ", ".join(transactions_payment.keys())
                transactions_payment_values = ", ".join(["?"] * len(transactions_payment))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_CRM_TRANSACTIONSPAYMENT ({transactions_payment_columns}) VALUES ({transactions_payment_values})", list(transactions_payment.values()))

                # DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT
                order_contract_id = random.randint(1, 1000000)
                ordered_contract = {
                    "ID": order_contract_id,
                    "CUSTOMER_ID": random.randint(1, 1000000),
                    "CONFIRMATION_CODE": confirmation_code,
                    "CREATED_AT": fake.date_time_this_decade(),
                    "SALES_INFO_ID": random.randint(1, 1000000),
                    "FILE_ARRIVING_DATE": fake.date(pattern="%Y-%m-%d"),
                    "STATUS": fake.text(max_nb_chars=64),
                    "DELIVERY_TYPE": fake.text(max_nb_chars=64),
                    "LAST_MODIFIED": fake.date_time_this_decade(),
                    "BRAND": random.randint(1, 1000),
                    "ORDERED_AT": fake.date_time_this_decade(),
                    "DELIVERY_ADDRESS_ID": random.randint(1, 1000000),
                    "OFFER": fake.text(max_nb_chars=1024),
                    "CORRECTION_FOR": fake.text(max_nb_chars=64),
                    "DEVICES_RETURNED_AT": fake.date_time_this_decade(),
                    "PURCHASE_ORDER_ID": fake.text(max_nb_chars=64),
                    "CDC_CODE": fake.random_int(min=1, max=10),

                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }

                ordered_contract_columns = ", ".join(ordered_contract.keys())
                ordered_contract_values = ", ".join(["?"] * len(ordered_contract))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT ({ordered_contract_columns}) VALUES ({ordered_contract_values})", list(ordered_contract.values()))

                variant_id = random.randint(1, 1000000)
                products_item_variant = {
                    "CART_IMAGE": fake.text(max_nb_chars=100),
                    "CODE": fake.text(max_nb_chars=64),
                    "COLOR_ID": random.randint(1, 1000000),
                    "COVER_IMAGE": fake.text(max_nb_chars=100),
                    "DISPLAY_ORDER": random.randint(1, 1000000),
                    "FILE_ARRIVING_DATE": fake.date(pattern="%Y-%m-%d"),
                    "ID": variant_id,
                    "ITEM_MODEL_ID": random.randint(1, 1000000),
                    "NAME_PRODUCTSITEMVARIANT": fake.text(max_nb_chars=64),
                    "OUT_OF_STOCK": random.randint(0, 1),
                    "OUT_OF_STOCK_ACTION": fake.text(max_nb_chars=64),
                    "STATUS": random.randint(0, 1),

                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                products_item_variant_columns = ", ".join(products_item_variant.keys())
                products_item_variant_values = ", ".join(["?"] * len(products_item_variant))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_PC_PRODUCTSITEMVARIANT ({products_item_variant_columns}) VALUES ({products_item_variant_values})", list(products_item_variant.values()))

                ordered_contracts_ordered_device = {
                    "ID": random.randint(1, 1000000),
                    "CONTRACT_ID": order_contract_id,
                    "TYPE_ID": variant_id,
                    "LAST_MODIFIED": fake.date_time_this_decade(),
                    "PRICE": round(random.uniform(0, 100000), 6),
                    "ARTICLE_ID": fake.text(max_nb_chars=4096),
                    "DATA": fake.text(max_nb_chars=5500),
                    "QUANTITY": random.randint(1, 1000),
                    "CDC_CODE": random.randint(-128, 127),
                    "FILE_ARRIVING_DATE": fake.date(pattern="%Y-%m-%d"),
                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                ordered_contracts_ordered_device_columns = ", ".join(ordered_contracts_ordered_device.keys())
                ordered_contracts_ordered_device_values = ", ".join(["?"] * len(ordered_contracts_ordered_device))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE ({ordered_contracts_ordered_device_columns}) VALUES ({ordered_contracts_ordered_device_values})", list(ordered_contracts_ordered_device.values()))

                ordered_contracts_ordered_simcard = {
                    "ID": random.randint(1, 1000000),
                    "PRICE": round(random.uniform(0, 100000), 6),
                    "CONTRACT_ID": order_contract_id,
                    "LAST_MODIFIED": fake.date_time_this_decade(),
                    "IMSI": fake.text(max_nb_chars=129),
                    "ITEM": fake.text(max_nb_chars=100),
                    "ARTICLE_ID": fake.text(max_nb_chars=128),

                    "MODIFICATION_TYPE": fake.random_element(elements=("I", "U", "D")),
                    "LOAD_ID": load_id,
                    "BATCH_ID": batch_id,
                    "REF_KEY": ref_key,
                    "INS_DTTM": datetime.now(),
                    "UPD_DTTM": datetime.now()
                }
                ordered_contracts_ordered_simcard_columns = ", ".join(ordered_contracts_ordered_simcard.keys())
                ordered_contracts_ordered_simcard_values = ", ".join(["?"] * len(ordered_contracts_ordered_simcard))
                cur.execute(f"INSERT INTO STG_ONLINE.DBSS_OM_ORDEREDCONTRACTSORDEREDSIMCARD ({ordered_contracts_ordered_simcard_columns}) VALUES ({ordered_contracts_ordered_simcard_values})", list(ordered_contracts_ordered_simcard.values()))

                ###############################################
                ref_key += 1
                trx_id += 1

            audit_table('DIRECT SALES', cur, load_id, batch_id, 'JSON_SALES_STG', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_CRM_TRANSACTIONSTRANSACTION', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_CRM_TRANSACTIONSPAYMENT', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_PC_PRODUCTSITEMVARIANT', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE', ref_key)
            audit_table('DIRECT SALES', cur, load_id, batch_id, 'DBSS_OM_ORDEREDCONTRACTSORDEREDSIMCARD', ref_key)
            con.commit()

            print(f"{ref_key}, rows inserted into JSON_SALES_STG")
            print(f"{ref_key}, rows inserted into DBSS_CRM_TRANSACTIONSTRANSACTION")
            print(f"{ref_key}, rows inserted into DBSS_CRM_TRANSACTIONSPAYMENT")
            print(f"{ref_key}, rows inserted into DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT")
            print(f"{ref_key}, rows inserted into DBSS_PC_PRODUCTSITEMVARIANT")
            print(f"{ref_key}, rows inserted into DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE")
            print(f"{ref_key}, rows inserted into DBSS_OM_ORDEREDCONTRACTSORDEREDSIMCARD")


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

    audit_table('CSO', cursor, load_id, batch_id, 'CSO_ADDRESS', row_count)
    audit_table('CSO', cursor, load_id, batch_id, 'CSO_NEW_PERSON', row_count)


def audit_table(source_system, cursor, load_id, batch_id, table_name, row_count):
    cdc_audit = f"""INSERT INTO GDEV1_ETL.CDC_AUDIT  
                        (SOURCE_NAME, LOAD_ID, BATCH_ID, TABLE_NAME, PROCESSED, RECORDS_COUNT)  
                        VALUES('{source_system}','{load_id}',{batch_id},'{table_name}',0,{row_count});"""
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
    djezzy_fake_data(10, 0, teradata_conn_info)
