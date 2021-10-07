import os
import uuid

from tabulate import tabulate

from DbConnector import DbConnector
from task_1.Activity import Activity

"""
Handles the database setup.
- Creates the tables
- Inserts data from the files
- Prints data from the database
"""


class DatabaseSetup:

    def __init__(self, connection: DbConnector):
        self.connection = connection
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.labels_dict = {}
        # dict will have the form:
        # {
        #   82: {
        #       ('2009-06-25 04:16:31', '2009-06-25 16:19:02'): "walk",
        #       ('2009-06-25 04:16:31', '2009-06-25 16:19:02'): "taxi"
        #       },
        #   83: {
        #       (date1, date2): "bus",
        #       (date3, date4): "run",
        #        ...
        #       }
        #   ...
        # }
    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS USER (
                   id VARCHAR(50) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN);
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS ACTIVITY (
                   id varchar(128) NOT NULL PRIMARY KEY,
                   user_id VARCHAR(50) NOT NULL,
                   FOREIGN KEY (user_id) REFERENCES test_db.USER(id),
                   transportation_mode VARCHAR(30), 
                   start_date_time DATETIME,
                   end_date_time DATETIME);
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_track_point_table(self):
        query = """CREATE TABLE IF NOT EXISTS TRACK_POINT (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id varchar(128) NOT NULL,
                   FOREIGN KEY (activity_id) REFERENCES test_db.ACTIVITY(id),
                   lat DOUBLE,
                   lon DOUBLE, 
                   altitude INT,
                   data_days DOUBLE, 
                   data_time DATETIME);
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def print_users(self):
        query = "SELECT * FROM test_db.USER"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Data from table USERS tabulated:\n{tabulate(rows, headers=self.cursor.column_names)}")

    def print_activity(self):
        query = "SELECT * FROM test_db.ACTIVITY"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Data from table USERS tabulated:\n{tabulate(rows, headers=self.cursor.column_names)}")

    def create_tables(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_track_point_table()

    def drop_tables(self):
        print('Are you sure you would like to drop the tables? (Y/N)')
        decision = input()
        if decision == "Y" or decision == "y":
            query = """DROP TABLE test_db.USER, test_db.ACTIVITY, test_db.TRACK_POINT CASCADE """
            self.cursor.execute(query)
            print('Tables dropped')
        else:
            print('No tables were dropped')

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def is_plt_file(self, extension):
        return extension == ".plt"

    def get_extension(self, path):
        name, extension = os.path.splitext(path)
        return extension

    def get_nr_of_lines(self, path):
        """
        Returns the number of lines in the file
        @param path: The path of the file
        @type path: str
        @return: Number of lines
        @rtype: int
        """
        reoccurring_lines = 6  # The 6 lines in the top which are not trajectories
        num_lines = sum(1 for line in open(path)) - reoccurring_lines
        return num_lines

    def get_user_label(self):
        """
        Returns the user labels in a list. Loads the entire file into memory and should not be used on large files.
        @return: A list of labels
        @rtype: list
        """
        label_path = "dataset/dataset/labeled_ids.txt"
        labels = open(label_path, 'r').read().splitlines()
        return labels

    def get_user_ids(self):
        """
        Searches the dataset folder for user ids and returns a sorted list of user ids.
        @return: A sorted list of user ids
        @rtype: list
        """
        path = "dataset/dataset/Data"
        user_ids = sorted([f for f in os.listdir(path) if not f.startswith('.')])
        return user_ids

    def insert_users(self):
        """
        Inserts users to the database
        @return: None
        @rtype: None
        """
        user_labels = self.get_user_label()
        user_ids = self.get_user_ids()

        try:
            for user in user_ids:
                has_label = False
                if user in user_labels:
                    has_label = True
                query = "INSERT INTO test_db.USER (id, has_labels) VALUES ('%s', %s)"
                self.cursor.execute(query % (user, has_label))
            self.db_connection.commit()
        except Exception as e:
            print(f'An error occurred while inserting users:{e}')

    def get_last_line(self, root: str, file: str):
        """
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: The last line in the file
        @rtype: str
        """
        try:
            path = os.path.join(root, file)  # The current path
            with open(path, "r") as f1:
                return f1.readlines()[-1]
        except Exception as e:
            print(f'An error occurred while retrieving the last line in the file:{e}')

    def get_first_line(self, root: str, file: str):
        """
        Returns the first relevant line
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: The first relevant line
        @rtype: str
        """
        try:
            with open(os.path.join(root, file)) as f:
                return_line = ""
                for read in range(7):
                    return_line = f.readline()  # removes the first lines containing descriptions.
                return return_line
        except Exception as e:
            print(f'An error occurred while retrieving the first line in the file:{e}')

    def format_label_line(self, label: str):
        """
        Formats the label line in a file to the values we need to insert into the database.
        @param label: The label line
        @type label: str
        @return: start_time, end_time and transportation_mode
        @rtype: str
        """
        values = label.split()
        start_time = "".join((values[0], " ", values[1]))
        end_time = "".join((values[2], " ", values[3]))
        transportation_mode = "".join(values[4])
        start_time = start_time.replace("/", "-")
        end_time = end_time.replace("/", "-")
        return start_time, end_time, transportation_mode

    def format_trajectory_time(self, label: str):
        values = label.split(",")
        time = "".join((values[5], " ", values[6]))
        return time

    def format_trajectory_line(self, line: str):
        """
        Formats the trajectory line in a file
        @param line: The trajectory line
        @type line: str
        @return: latitude, longitude, altitude, days_passed and start_time
        @rtype: str
        """
        values = line.split(",")
        latitude = values[0]
        longitude = values[1]
        altitude = values[3]
        days_passed = values[4]
        start_time = "".join((values[5].replace('-', '/'), " ", values[6]))
        return latitude, longitude, altitude, days_passed, start_time

    def create_activity(self, root, file):
        """
        Creaetes a new activity
        @param root: The path of the file
        @type root: str
        @param file: The name of the file
        @type file:str
        @return: Activity - a new activity
        @rtype: Activity
        """
        user_id = os.path.basename(os.path.dirname(root))
        first_line = self.get_first_line(root, file)
        last_line = self.get_last_line(root, file)
        start_time = self.format_trajectory_time(first_line)
        end_time = self.format_trajectory_time(last_line)
        activity_id = str(uuid.uuid4())
        if (start_time, end_time) in self.labels_dict[user_id]:
            transportation_mode = self.labels_dict[user_id][(start_time, end_time)]
            return Activity(activity_id, user_id, start_time, end_time, transportation_mode)
        else:
            return Activity(activity_id, user_id, start_time, end_time, None)

    def create_label_activities(self):
        """
        Creates a new label activity.
        @return: label_activity_list - a list with all label activities
        @rtype: list
        """
        label_activity_list = []
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            for file in files:
                if file == "labels.txt":
                    with open(os.path.join(root, file)) as f:
                        f.readline()  # removes the first line containing descriptions.
                        user_id = os.path.basename(os.path.basename(root))
                        # if user-id (e.g. 082) isn't a key in label-dict, add it empty dict as value
                        if user_id not in self.labels_dict:
                            self.labels_dict[user_id] = {}
                        for line in f:
                            start_time, end_time, transportation_mode = self.format_label_line(line)
                            # Assuming label dates (together) are unique => add label-dates-tuple as key, with
                            # transportation mode as value
                            self.labels_dict[user_id][(start_time, end_time)] = transportation_mode

    def traverse_dataset(self):
        """
        Traverses the dataset and inserts activities and track_points
        @return: None
        @rtype: None
        """

        # populate label_dict
        self.create_label_activities()
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            if len(dirs) == 0 and len(files) > 0:
                for file in files:
                    path = os.path.join(root, file)  # The current path
                    if self.is_plt_file(self.get_extension(path)) and self.get_nr_of_lines(path) <= 2500:
                        activity = self.create_activity(root, file)
                        self.insert_activity(activity)  # Inserts the activity into the database
                        track_point_list = []  # A list to batch insert the trajectories
                        with open(os.path.join(root, file)) as f:  # opens the current file
                            for read in range(6):
                                f.readline()

                            for line in f:
                                latitude, longitude, altitude, days_passed, start_time = \
                                    self.format_trajectory_line(line)
                            track_point_list.append(
                                (activity.id, latitude, longitude, altitude, days_passed, start_time))
                        self.batch_insert_track_points(track_point_list)  # Batch insert the track points in this file

    def batch_insert_activities(self, label_activity_list: list):
        """
        Batch inserts activities into the database
        @param label_activity_list: The list with all label activities
        @type label_activity_list: list
        @return: None
        @rtype: None
        """
        try:
            activity_query = """INSERT INTO test_db.ACTIVITY (id, user_id, transportation_mode, start_date_time, end_date_time) 
                                    VALUES (%s, %s, %s, %s, %s)"""
            self.cursor.executemany(activity_query, label_activity_list)
            self.db_connection.commit()
        except Exception as e:
            print(f'An error occurred while batch inserting activities:{e}')

    def insert_activity(self, activity: Activity):
        """
        Inserts a single activity
        @param activity: The activity that should be inserted into the database.
        @type activity: Activity
        @return: None
        @rtype: None
        """
        try:
            query = """INSERT INTO test_db.ACTIVITY (id, user_id, start_date_time, end_date_time) 
                                VALUES ('%s', '%s','%s','%s')"""
            self.cursor.execute((query % (
                activity.id, activity.user_id, activity.start_date_time, activity.end_date_time)))
            self.db_connection.commit()
        except Exception as e:
            print(f'An error occurred while inserting Activity:{e}')

    def batch_insert_track_points(self, track_points: list):
        """
        Batch insert users into the database
        @param track_points:
        @type track_points:
        @return: None
        @rtype: None
        """
        try:
            trajectory_query = """INSERT INTO test_db.TRACK_POINT (activity_id, lat, lon, altitude, data_days, data_time) 
                                  VALUES (%s, %s, %s, %s, %s, %s)"""
            self.cursor.executemany(trajectory_query, track_points)
            self.db_connection.commit()
        except Exception as e:
            print(f'An error occurred while batch inserting track points:{e}')

    # THE FOLLOWING METHOD MIGHT BE UNNECESSARY (see insert_activity)
    '''def alter_activites_insert_transp_mode(self):
        # iterate over (user-id, dict of dates/labels)
        for user_id, label in self.labels_dict.items():
            # get all the date pairs from the current user in iteration
            get_activity_dates_query = """SELECT 
                                            test_db.ACTIVITY.start_date_time
                                            test_db.ACTIVITY.end_date_time
                                          FROM test_db.ACTIVITY
                                          WHERE 
                                            test_db.ACTIVITY.user_id = %s"""
            self.cursor.execute(get_activity_dates_query % user_id)
            # list of tuples??
            activity_start_end_dates = self.cursor.fetchall()
            # iterate over date, transportation_label in current users dict
            for label_start_end, transp_mode in label.items():
                # if the user has the (exact) label dates in their activity dates
                if label_start_end in activity_start_end_dates:
                    # update the transportation mode for that activity
                    alter_transp_mode_query = """UPDATE test_db.ACTIVITY
                                                    SET test_db.transportation_mode = %s
                                                 WHERE
                                                    test_db.ACTIVITY.user_id = %s
                                                 AND 
                                                    test_db.ACTIVITY.start_date_time = %s
                                                 AND 
                                                    test_db.ACTIVITY.end_date_time = %s
                                                    """
                    self.cursor.execute(alter_transp_mode_query % (transp_mode, user_id, label_start_end[0], label_start_end[1]))
        try:
            pass
        except Exception as e:
            print(f'An error occured while altering activities to insert transportation modes\nfrom labels:{e}')'''
