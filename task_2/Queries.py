from tabulate import tabulate
from haversine import haversine
import DbConnector


class Queries:

    def __init__(self, connection: DbConnector):
        self.connection = connection
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def select_nr_of_users(self):
        query = "SELECT count(*) FROM test_db.USER"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of USER:\n {tabulate(rows)}")

    def select_nr_of_activities(self):
        query = "SELECT count(*) FROM test_db.ACTIVITY"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of ACTIVITY:\n {tabulate(rows)}")

    def select_nr_of_track_points(self):
        query = "SELECT count(*) FROM test_db.TRACK_POINT"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of TRACK_POINTS:\n {tabulate(rows)}")

    def select_average_nr_of_activities(self):
        query = """SELECT 
                   (SELECT COUNT(*) FROM test_db.ACTIVITY)  /  (SELECT COUNT(*) FROM test_db.USER) 
    AS average_lists_per_user ;  """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Amount of Average:\n {tabulate(rows)}")

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

    def select_never_taxi_user(self):

        query = """SELECT distinct test_db.ACTIVITY.user_id, test_db.ACTIVITY.transportation_mode
                    FROM test_db.ACTIVITY
                    where transportation_mode != 'taxi'

                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Users who have never used taxi:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

    def select_nr_used_transportation(self):
        query = """SELECT distinct test_db.ACTIVITY.transportation_mode, count(distinct test_db.ACTIVITY.user_id)
                    FROM test_db.ACTIVITY, test_db.USER
                    where transportation_mode != 'None' AND test_db.ACTIVITY.user_id = test_db.USER.id
                    group by test_db.ACTIVITY.transportation_mode

                 """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(
            f"Distinct users on each activity:"
            f"\n {tabulate(rows, headers=self.cursor.column_names)}")

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

    ## UNFINISHED!!!!
    def tot_dist_in_2008_by_user_112(self):
        activity_ids_query = """SELECT test_db.ACTIVITY.id FROM test_db.ACTIVITY 
                                    WHERE test_db.ACTIVITY.user_id= 112
                                    AND  year(test_db.ACTIVITY.start_date_time) = 2008;"""

        self.cursor.execute(activity_ids_query)
        activity_ids = self.cursor.fetchall()

        distances = []
        for act_id in activity_ids:

            lat_lon_query = """SELECT test_db.TRACK_POINT.lat as latitude, test_db.TRACK_POINT.lon as longitude
                                            FROM test_db.TRACK_POINT
                                            WHERE test_db.TRACK_POINT.activity_id = %s"""
            self.cursor.execute(lat_lon_query % act_id)
            lat_lons_for_activity = self.cursor.fetchall()
            for i in range(0, len(lat_lons_for_activity) - 1):
                distances.append(haversine(lat_lons_for_activity[i], lat_lons_for_activity[i + 1]))


