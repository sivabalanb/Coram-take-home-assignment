import time
from datetime import datetime, timedelta
import sqlalchemy as sa
import random

def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

    # Create the database engine
    # engine = sa.create_engine(db_uri)

    for attempt in range(5):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == 4:
                print("error", e)
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str):
    
    # Convert the timestamp to a datetime object
    # timestamp_dt = datetime.datetime.fromisoformat(timestamp)
    # Insert data into the 'events' table
    conn.execute(
        sa.text("INSERT INTO events (time, type) VALUES (:timestamp, :event_type)"),
        {"timestamp": timestamp, "event_type": event_type},
    )
    

def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:

    select_query = sa.text("WITH event_diff AS (SELECT type, time, LAG(time) OVER (PARTITION BY type ORDER BY time) AS prev_time, EXTRACT(EPOCH FROM (time - LAG(time) OVER (PARTITION BY type ORDER BY time))) AS time_diff FROM events) SELECT type, MIN(time) AS start_time, MAX(time) AS end_time FROM (SELECT type, time, SUM(CASE WHEN time_diff > 30 OR time_diff IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY type ORDER BY time) AS grp FROM event_diff) grouped GROUP BY type, grp ORDER BY start_time;")
    result = conn.execute(select_query)
    # Initialize an empty dictionary
    result_dict = {}

    # Iterate through the list of tuples
    for item in result:
        # Extract values from the tuple
        type, start_time, end_time = item

        # Convert datetime objects to string format
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')

        # Check if the 'type' key exists in the dictionary
        if type in result_dict:
            # Append the tuple to the existing list
            result_dict[type].append((start_time_str, end_time_str))
        else:
            # Create a new list for the 'type' key
            result_dict[type] = [(start_time_str, end_time_str)]

    # Print the resulting dictionary
    return result_dict

def generate_random_event():
    event_types = ["pedestrian", "bicycle", "car", "truck", "van"]
    timestamp = datetime.datetime.now().isoformat()
    event_type = random.choice(event_types)
    return timestamp, event_type

def check_pedestrian_sequence(events):
    pedestrian_count = 0
    last_timestamp = None

    for timestamp, event_type in events:
        current_timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

        if event_type == "pedestrian":
            if last_timestamp is not None and (current_timestamp - last_timestamp).total_seconds() <= 30:
                pedestrian_count += 1
            else:
                pedestrian_count = 1

            last_timestamp = current_timestamp
        else:
            pedestrian_count = 0

        if pedestrian_count >= 5:
            print("Continuous sequence of 'pedestrian' detected:", pedestrian_count, "occurrences.")

def main():
    conn = database_connection()

    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]
    check_pedestrian_sequence(events)
    for timestamp, event_type in events:
        ingest_data(conn, timestamp, event_type)

    aggregate_results = aggregate_events(conn)
    print(aggregate_results)

    conn.commit()
if __name__ == "__main__":
    main()
