from tabulate import tabulate
from haversine import haversine
import DbConnector
import pandas as pd


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
        query = """SELECT user_id, my_count AS counted_activities
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

    # nr 6
    def covid_tracking(self):
        close_people_counter = 0
        users_query = """SELECT test_db.USER.id FROM test_db.USER"""
        self.cursor.execute(users_query)
        user_ids = [i[0] for i in self.cursor.fetchall()]

        for i in range(0, len(user_ids) - 1):
            # Get all trackpoints for one user n
            query = """SELECT 
                            test_db.ACTIVITY.user_id, 
                            test_db.TRACK_POINT.data_days, 
                            test_db.TRACK_POINT.lat, 
                            test_db.TRACK_POINT.lon
                        FROM test_db.ACTIVITY
                        JOIN test_db.TRACK_POINT
                            ON test_db.ACTIVITY.id = test_db.TRACK_POINT.activity_id
                            WHERE test_db.ACTIVITY.user_id = %s
                        ORDER BY test_db.TRACK_POINT.lat, test_db.TRACK_POINT.lon;"""
            self.cursor.execute(query % user_ids[i])
            first_join_table = self.cursor.fetchall()

            # Get trackpoints for all subsequent users (n+1)...m
            for j in range(i + 1, len(user_ids) - 2):
                query = """SELECT 
                                test_db.ACTIVITY.user_id, 
                                test_db.TRACK_POINT.data_days, 
                                test_db.TRACK_POINT.lat, 
                                test_db.TRACK_POINT.lon
                            FROM test_db.ACTIVITY
                            JOIN test_db.TRACK_POINT
                                ON test_db.ACTIVITY.id = test_db.TRACK_POINT.activity_id
                                WHERE test_db.ACTIVITY.user_id = %s
                            ORDER BY test_db.TRACK_POINT.lat, test_db.TRACK_POINT.lon;"""
                self.cursor.execute(query % user_ids[j])
                second_join_table = self.cursor.fetchall()

                # Check proximity between two current  users
                for k in first_join_table:
                    for l in second_join_table:
                        seconds_diff = abs(k[1] - l[1]) * 24 * 3600
                        distance_diff = haversine((k[2], k[3]), (l[2], l[3]), unit='m')
                        # print(k[0], "-->", l[0], " sec: ", seconds_diff, "dist: ", distance_diff)
                        if (seconds_diff <= 60) and (distance_diff <= 100):
                            close_people_counter += 1
        print(f"Number of people in close proximity of eachother : {close_people_counter}\n")

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
        query = """SELECT distinct month(test_db.ACTIVITY.start_date_time) as start_time_year, 
                    count(test_db.ACTIVITY.user_id) as nr_of_activities
                    FROM test_db.ACTIVITY
                    WHERE 
                    year(test_db.ACTIVITY.start_date_time) = 2008
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
                    month(test_db.ACTIVITY.start_date_time) = 11
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
        get_users_query = """select test_db.USER.id
                    from test_db.USER
                    order by test_db.USER.id
        """
        get_altitude_query = """SELECT
                   greatest(test_db.TRACK_POINT.altitude - lag(test_db.TRACK_POINT.altitude) 
                   over (order by test_db.TRACK_POINT.activity_id), 0) as altitude_gain
                   from test_db.TRACK_POINT, test_db.ACTIVITY
                   where test_db.ACTIVITY.user_id = %s
                   and test_db.TRACK_POINT.activity_id = test_db.ACTIVITY.id
                   and test_db.TRACK_POINT.altitude != -777
                """

        self.cursor.execute(get_users_query)
        rows = self.cursor.fetchall()
        user_list = []
        gain_list = []
        for user in rows:
            self.cursor.execute(get_altitude_query % user[0])
            gain = self.cursor.fetchall()
            user_list.append(user)
            data = pd.DataFrame(data=gain)
            sum = data.sum(axis=0, skipna=True)
            if len(sum) > 0:
                meters = sum[0] / 3.2808
                gain_list.append(meters)
                print(f'user: {user[0]} and meters: {meters}')
            else:
                meters = "Unknown because of 2500 line limit"
                gain_list.append(meters)
                print(f'user{user[0]} and meters: {meters}')

        value_list = {'user': user_list,
                      'altitude gain': gain_list}
        dataframe = pd.DataFrame(data=value_list)
        dataframe.sort_values(by=['altitude gain'], inplace=True, ascending=False)
        print(dataframe.head(20))

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
