from DbConnector import DbConnector
from task_1.DatabaseSetup import DatabaseSetup


def task_1():
    try:
        # Creates a connection with the database
        connector = DbConnector()
        # Creates a database setup object
        setup = DatabaseSetup(connector)
        # Drops the table if it is already created
        setup.drop_tables()
        # Creates the tables if they don't already exist
        setup.create_tables()
        # Insert user.
        setup.insert_users()
        # Inserts activities and track points
        setup.traverse_dataset()
    except Exception as e:
        print(f'An error occurred in task_1:{e}')


def task_2():
    print('task 2')


def task_3():
    print('task 3')


def main():
    task_1()


if __name__ == "__main__":
    main()
