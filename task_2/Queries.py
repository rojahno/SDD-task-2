from tabulate import tabulate
from haversine import haversine
import DbConnector


class Queries:

    def __init__(self, connection: DbConnector):
        self.connection = connection
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

        # Nr 1

    def select_nr_of_users(self):
        query = "SELECT count(*) FROM test_db.USER"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of USER:\n {tabulate(rows)}")

        # Nr 1

    def select_nr_of_activities(self):
        query = "SELECT count(*) FROM test_db.ACTIVITY"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of ACTIVITY:\n {tabulate(rows)}")

        # Nr 1

    def select_nr_of_track_points(self):
        query = "SELECT count(*) FROM test_db.TRACK_POINT"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of TRACK_POINTS:\n {tabulate(rows)}")

        # Nr 2

    def select_average_nr_of_activities(self):
        query = """SELECT 
                   (SELECT COUNT(*) FROM test_db.ACTIVITY)  /  (SELECT COUNT(*) FROM test_db.USER) 
    AS average_lists_per_user ;  """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of Average:\n {tabulate(rows)}")

        # Nr 2

    def select_min_nr_of_activities(self):
        query = """SELECT min(my_count) AS min_count
               FROM (SELECT test_db.ACTIVITY.user_id, count(test_db.ACTIVITY.user_id) AS my_count
               FROM test_db.ACTIVITY, test_db.USER
                WHERE (test_db.ACTIVITY.user_id = test_db.USER.id)
                GROUP BY user_id) AS test2 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Min amount of activities per USER:\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 2
    def select_max_nr_of_activities(self):
        query = """SELECT MAX(my_count) AS min_count
               FROM (SELECT test_db.ACTIVITY.user_id, count(test_db.ACTIVITY.user_id) AS my_count
               FROM test_db.ACTIVITY, test_db.USER
                WHERE (test_db.ACTIVITY.user_id = test_db.USER.id)
                GROUP BY user_id) AS count """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Max amount of activities per USER:\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 3
    def select_ten_max_nr_of_activities(self):
        query = """SELECT user_id, my_count AS min_count
               FROM (SELECT test_db.ACTIVITY.user_id AS user_id, count(test_db.ACTIVITY.user_id) AS my_count
               FROM test_db.ACTIVITY, test_db.USER
               WHERE (test_db.ACTIVITY.user_id = test_db.USER.id)
               GROUP BY user_id ) AS count
               ORDER BY my_count DESC 
               LIMIT 10"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Max 10 amount of activities of USER:\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 4
    def select_nr_users_with_multiple_day_activities(self):  # very good function name :))
        query = """SELECT COUNT(DISTINCT user_id)
               FROM test_db.ACTIVITY
               where (select CAST(test_db.ACTIVITY.start_date_time AS DATE) 
               != (SELECT CAST(test_db.ACTIVITY.end_date_time as DATE)))
               """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Users who have started an activity one day, finished it another:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 5
    def select_reoccurring_activities(self):
        # todo test this function. unsure if correct
        query = """SELECT test_db.ACTIVITY.start_date_time, COUNT(test_db.ACTIVITY.start_date_time),
                 test_db.ACTIVITY.end_date_time, COUNT(test_db.ACTIVITY.end_date_time)
                 FROM test_db.ACTIVITY
                 GROUP BY test_db.ACTIVITY.start_date_time, test_db.ACTIVITY.end_date_time
                 HAVING COUNT(test_db.ACTIVITY.start_date_time) > 1 AND
                        COUNT(test_db.ACTIVITY.end_date_time) > 1
                 ORDER BY test_db.ACTIVITY.start_date_time ASC 
                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Repeated activities:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 7
    def select_never_taxi_user(self):
        query = """SELECT distinct test_db.ACTIVITY.user_id
                    FROM test_db.ACTIVITY
                    where test_db.ACTIVITY.user_id not in (
                        select test_db.ACTIVITY.user_id as id
                        from test_db.ACTIVITY
                        where transportation_mode = 'taxi')
order by test_db.ACTIVITY.user_id

                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Users who have never used taxi:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 8
    def select_nr_used_transportation(self):
        query = """SELECT distinct test_db.ACTIVITY.transportation_mode, count(distinct test_db.ACTIVITY.user_id)
                    FROM test_db.ACTIVITY, test_db.USER
                    WHERE test_db.ACTIVITY.transportation_mode IS NOT NULL and test_db.ACTIVITY.user_id = test_db.USER.id
                    group by test_db.ACTIVITY.transportation_mode

                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Distinct users on each activity:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 9a
    def select_year_with_most_activities(self):
        query = """SELECT distinct year(test_db.ACTIVITY.start_date_time) as start_time_year, count(test_db.ACTIVITY.user_id) as nr_of_activities
                        FROM test_db.ACTIVITY
                        group by year(test_db.ACTIVITY.start_date_time)
                        order by nr_of_activities desc 
                        LIMIT 1
                     """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"The year with most activities:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 9a
    def select_month_with_most_activities(self):
        # Remove limit 1 to see the whole list
        query = """SELECT distinct month(test_db.ACTIVITY.start_date_time) as start_time_year, count(test_db.ACTIVITY.user_id) as nr_of_activities
                    FROM test_db.ACTIVITY
                    group by start_time_year
                    order by nr_of_activities desc 
                    LIMIT 1
                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"The month with most activities:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 9 b
    def select_user_with_most_activities_this_year(self):
        query = """SELECT test_db.ACTIVITY.user_id as users,
                    count(test_db.ACTIVITY.id) as nr_of_activities,
                    sum(time_to_sec(timediff(end_date_time, start_date_time))/3600) as sum_hours
                FROM test_db.ACTIVITY
                WHERE 
                    year(test_db.ACTIVITY.start_date_time) = 2008
                 and 
                    month(test_db.ACTIVITY.start_date_time) = 5
                group by user_id
                order by nr_of_activities desc
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"The user with the most activities:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr 10
    def tot_dist_in_2008_by_user_112(self):
        query = """select test_db.TRACK_POINT.lat, test_db.TRACK_POINT.lon 
                        from test_db.TRACK_POINT join test_db.ACTIVITY on test_db.ACTIVITY.id=test_db.TRACK_POINT.id 
                        where test_db.ACTIVITY.user_id='112' and 
                                YEAR(test_db.TRACK_POINT.data_time)='2008' and 
                                test_db.ACTIVITY.transportation_mode='walk'
                        """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        total_distance = 0
        for i in range(1, len(rows)):
            total_distance += haversine(rows[i - 1], rows[i])

        print("Total walked distance for user 112 in 2008 in :", total_distance)

    # Nr 11
    def select_top_20_users_with_most_gained(self):
        query = """SELECT distinct test_db.ACTIVITY.user_id, 
                    (count(test_db.TRACK_POINT.altitude))/3 as nr_of_altitude_gained
                FROM test_db.ACTIVITY right join test_db.TRACK_POINT ON ACTIVITY.user_id = TRACK_POINT.altitude
                WHERE test_db.TRACK_POINT.altitude != -777
                group by user_id
                order by nr_of_altitude_gained desc
                LIMIT 20
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"The top 20 user with the most altitude gained:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    # Nr. 12
    def select_all_users_with_invalid_activities(self):
        query = """SELECT a.user_id, count(DISTINCT tp1.activity_id)
                FROM test_db.TRACK_POINT tp1 
                INNER JOIN test_db.TRACK_POINT tp2 ON tp2.id = tp1.id+1
                AND tp2.activity_id = tp1.activity_id
                INNER JOIN test_db.ACTIVITY a ON a.id = tp1.activity_id
                WHERE tp1.data_time <= SUBDATE(tp2.data_time,INTERVAL 5 minute) 
                AND (tp1.activity_id = a.id)
                GROUP BY user_id
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"All users with invalid activities:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")
