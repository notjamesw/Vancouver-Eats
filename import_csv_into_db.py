import os
import psycopg2
import pandas as pd

DB_HOST = "localhost"
DB_NAME = "vancouver_eats"
DB_USER = "jiayangdong"
DB_PASSWORD = "postgres"

def import_csv_to_db(csv_file, connection):
    try:
        df = pd.read_csv(csv_file)
        cursor = connection.cursor()

        for _, row in df.iterrows():
            sql = """
                INSERT INTO my_table (
                    input_id, link, title, category, address, open_hours, 
                    popular_times, website, phone, plus_code, review_count, 
                    review_rating, reviews_per_rating, latitude, longitude, cid, 
                    status, descriptions, reviews_link, thumbnail, timezone, 
                    price_range, data_id, images, reservations, order_online, 
                    menu, owner, complete_address, about, user_reviews
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (cid) DO NOTHING;
            """

            cursor.execute(sql, (
                row['input_id'], row['link'], row['title'], row['category'], row['address'],
                row['open_hours'], row['popular_times'], row['website'], row['phone'], 
                row['plus_code'], row['review_count'], row['review_rating'], 
                row['reviews_per_rating'], row['latitude'], row['longitude'], 
                row['cid'], row['status'], row['descriptions'], row['reviews_link'], 
                row['thumbnail'], row['timezone'], row['price_range'], row['data_id'], 
                row['images'], row['reservations'], row['order_online'], row['menu'], 
                row['owner'], row['complete_address'], row['about'], row['user_reviews']
            ))

        connection.commit()
        print(f"Imported {csv_file} successfully.")

    except Exception as e:
        print(f"Error importing {csv_file}: {e}")
        connection.rollback()

def main():
    connection = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    # print("connected!")

    csv_dir = "/Users/jiayangdong/Desktop/scraped-businesses" # change this to the location of the csv files!!

    try:
        for file in os.listdir(csv_dir):
            if file.endswith(".csv"):
                import_csv_to_db(os.path.join(csv_dir, file), connection)
    finally:
        connection.close()

if __name__ == "__main__":
    main()