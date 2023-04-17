from server.functions import *


def fake_data_cso_address(row_count):
    fake = Faker()
    # Connect to Teradata
    ip = "localhost"
    user = "dbc"
    password = "dbc"
    conn = teradatasql.connect(host=ip, user=user, password=password)
    cursor = conn.cursor()
    # Generate fake data and insert into table
    for i in range(row_count):
        cso_number = random.randint(100000000, 999999999)
        delivered_date = fake.date_time_between(start_date="-1y", end_date="now")
        address = fake.address()
        address_date = fake.date_time_between(start_date=delivered_date, end_date="now")
        police_station_id = random.randint(100, 999)
        governorate_id = random.randint(1, 20)
        ref_key = random.randint(100000000, 999999999)
        load_id = fake.uuid4()
        batch_id = random.randint(1, 5)
        modification_type = random.choice(["I", "U", "D"])
        is_transferred = random.randint(0, 1)
        data_extraction_date = fake.date_time_between(start_date=address_date, end_date="now")
        ins_dttm = fake.date_time_between(start_date="-1y", end_date="now")
        upd_dttm = fake.date_time_between(start_date=ins_dttm, end_date="now")

        insert_query = f"INSERT INTO STG_ONLINE.CSO_ADDRESS (CSO_NUMBER, DELIVERED_DATE, ADDRESS, ADDRESS_DATE, POLICE_STATION_ID, GOVERNORATE_ID, REF_KEY, LOAD_ID, BATCH_ID, MODIFICATION_TYPE, IS_TRANSFERRED, DATA_EXTRACTION_DATE, INS_DTTM, UPD_DTTM) " \
                       f"VALUES ({cso_number}, '{delivered_date}', '{address}', '{address_date}', {police_station_id}, {governorate_id}, {ref_key}, '{load_id}', {batch_id}, '{modification_type}', {is_transferred}, '{data_extraction_date}', '{ins_dttm}', '{upd_dttm}')"
        cursor.execute(insert_query)
    print(f"{row_count} rows of fake data generated and inserted into STG_ONLINE.CSO_ADDRESS table.")
    conn.close()


if __name__ == '__main__':
    fake_data_cso_address(100)
