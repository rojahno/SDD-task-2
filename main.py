from DbConnector import DbConnector
from task_1.DatabaseSetup import DatabaseSetup


def task_1():
    # Creates a connection with the database
    connector = DbConnector()
    # Creates a database setup object
    setup = DatabaseSetup(connector)
    # setup.drop_tables()
    # Creates the tables if they don't already exist
    # setup.create_tables()
    # Shows the tables
    setup.show_tables()
    # prints the user
    # setup.print_users()
    # Insert user.
    # setup.insert_users()
    setup.insert_activity()
    # setup.print_activity()
    # setup.insert_trajectory()


def task_2():
    print('task 2')


def task_3():
    print('task 3')


def main():
    task_1()


if __name__ == "__main__":
    main()
