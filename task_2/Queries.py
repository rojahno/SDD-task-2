from tabulate import tabulate

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


