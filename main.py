from DbConnector import DbConnector
from task_1.DatabaseSetup import DatabaseSetup
from task_2.Queries import Queries


def task_1():
    # Creates a connection with the database
    connector = DbConnector()
    # Creates a database setup object
    setup = DatabaseSetup(connector)
    setup.drop_tables()
    # Creates the tables if they don't already exist
    setup.create_tables()
    # Shows the tables
    setup.show_tables()
    # prints the user
    # setup.print_users()
    # Insert user.
    setup.insert_users()
    setup.insert_activity()
    # setup.print_activity()
    # setup.batch_insert_activities()
    # setup.batch_insert_track_points()


def task_2():
    # Creates a connection with the database
    connector = DbConnector()
    query = Queries(connector)

    # Selects the number of each table
    # query.select_nr_of_users()
    # query.select_nr_of_activities()
    # query.select_nr_of_track_points()

    # Selects average, min and max
    # query.select_average_nr_of_activities()
    # query.select_min_nr_of_activities()
    # query.select_max_nr_of_activities()

    # Find top 10 users with the highest amount of activities
    # query.select_ten_max_nr_of_activities()

    # Find user who have started an activity one day, and finished it the other
    # query.select_nr_users_with_multiple_day_activities()

    # Find activities that are registered multiple times
    # query.select_reoccurring_activities()

    # Find users who have never taken a taxi
    # query.select_never_taxi_user()

    # Find all types of transportation modes and count how many distinct users that
    # have used the different transportation modes
    # query.select_nr_used_transportation()

    # Find the year with most activities
    # query.select_year_with_most_activities()

    # The month with most activities
    # query.select_month_with_most_activities()

    # User with the most activities bis year and month
    # query.select_user_with_most_activities_this_year()

    #  Total distance in 2008 by user 112
    # query.tot_dist_in_2008_by_user_112()

    # Top 20 users ho have gained the most altitude
    # query.select_top_20_users_with_most_gained()

    # All users with invalid activities
    # query.select_all_users_with_invalid_activities()

    # query.test()

def task_3():
    print('task 3')


def main():
    task_1()


if __name__ == "__main__":
    main()
