from DbConnector import DbConnector
from tabulate import tabulate
import os

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

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS USER (
                   id VARCHAR(50) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN);
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS ACTIVITY (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
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
                   activity_id INT NOT NULL,
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
        """
        Todo change to one function " print_table(table_name)?
        """
        query = "SELECT * FROM test_db.USER"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print(f"Data from table USERS tabulated:\n{tabulate(rows, headers=self.cursor.column_names)}")

    def print_activity(self):
        """
        Todo change to one function " print_table(table_name)?
        """
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
        if extension == ".plt":
            return True
        return False

    def get_extension(self, path):
        name, extension = os.path.splitext(path)
        return extension

    def get_nr_of_lines(self, path):
        """
        todo Check if this could be done in a cheaper way.
        todo Currently O(n). Want to have Big O(1) if possible.
        """
        reoccurring_lines = 6  # The 6 lines in the top which are not trajectories
        num_lines = sum(1 for line in open(path)) - reoccurring_lines
        return num_lines

    def get_user_label(self):
        """todo change the way to fetch label ids"""
        label_path = "dataset/dataset/labeled_ids.txt"
        # This approach is not scalable, since it loads the entire dataset into memory.
        labels = open(label_path, 'r').read().splitlines()
        return labels

    def get_user_ids(self):
        path = "dataset/dataset/data"
        user_ids = sorted([f for f in os.listdir(path) if not f.startswith('.')])
        return user_ids

    def insert_users(self):
        """
        Todo add a try catch
        """
        user_labels = self.get_user_label()
        user_ids = self.get_user_ids()

        for user in user_ids:
            has_label = False
            if user in user_labels:
                has_label = True
            query = "INSERT INTO test_db.USER (id, has_labels) VALUES ('%s', %s)"
            # todo change to execute many? Test the speed difference?
            self.cursor.execute(query % (user, has_label))
            print(f'User:{user} & has_label: {has_label} ')
        self.db_connection.commit()

    def get_last_line(self, root: str, file: str):
        with open(os.path.join(root, file), "rb") as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            return f.readline().decode()

    def get_first_line(self, root: str, file: str):
        with open(os.path.join(root, file)) as f:
            return_line = ""
            for read in range(7):  # todo find another way to remove the first 6 lines
                return_line = f.readline()  # removes the first lines containing descriptions.
            return return_line

    def format_label_line(self, label: str):
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

    def insert_labels(self):
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            for file in files:
                if file == "labels.txt":
                    with open(os.path.join(root, file)) as f:
                        f.readline()  # removes the first lines containing descriptions.
                        user_id = os.path.basename(os.path.basename(root))
                        for line in f:
                            start_time, end_time, transportation_mode = self.format_label_line(line)
                            query = """INSERT INTO test_db.ACTIVITY (user_id, start_date_time, end_date_time, 
                                          transportation_mode) 
                                                      VALUES ('%s', '%s', '%s', '%s')"""
                            # Todo test execute many
                            self.cursor.execute(query % (user_id, start_time, end_time, transportation_mode))
        self.db_connection.commit()

    def insert_activity(self):
        """
        Todo add a try catch
        """
        self.insert_labels()
        for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
            if len(dirs) == 0 and len(files) > 0:
                for file in files:
                    path = os.path.join(root, file)  # The current path
                    extension = self.get_extension(path)  # The extension of the path.
                    nr_of_lines = self.get_nr_of_lines(path)  # The number of lines in the file.
                    if self.is_plt_file(extension) and nr_of_lines <= 2500:
                        user_id = os.path.basename(os.path.dirname(root))
                        first_line = self.get_first_line(root, file)
                        last_line = self.get_last_line(root, file)
                        start_time = self.format_trajectory_time(first_line)
                        end_time = self.format_trajectory_time(last_line)

                        query = """INSERT INTO test_db.ACTIVITY (user_id, start_date_time, end_date_time) 
                                       VALUES ('%s', '%s', '%s')"""
                        self.cursor.execute(query % (user_id, start_time, end_time))

        # Todo test the commit position
        self.db_connection.commit()


def insert_trajectory(self):
    for root, dirs, files in os.walk('dataset/dataset/Data', topdown=True):
        if len(dirs) == 0 and len(files) > 0:
            for file in files:
                path = os.path.join(root, file)  # The current path
                extension = self.get_extension(path)  # The extension of the path.
                nr_of_lines = self.get_nr_of_lines(path)  # The number of lines in the file.

                if self.is_plt_file(extension) and nr_of_lines <= 2500:  # Reduces  data points from 24 mill, to 16k
                    with open(os.path.join(root, file)) as f:
                        for read in range(6):  # todo find another way to remove the first 6 lines
                            test = f.readline()  # removes the first lines containing descriptions.
                            print(test)
                        for line in f:
                            values = line.split(",")
                            latitude = values[0]
                            longitude = values[1]
                            altitude = values[3]
                            days_passed = values[4]
                            start_time = "".join((values[5].replace('-', '/'), " ", values[6]))
                            print(start_time)

                # print(file)
