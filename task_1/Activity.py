class Activity:

    def __init__(self, id, user_id, start_date_time, end_date_time, transportation_mode=None):
        self.id = id
        self.user_id = user_id
        self.transportation_mode = transportation_mode
        self.start_date_time = start_date_time
        self.end_date_time = end_date_time

    def set_id(self, id):
        self.id = id

    def set_user_id(self, user_id):
        self.user_id = user_id

    def set_transportation_mode(self, transportation_mode):
        self.transportation_mode = transportation_mode

    def set_start_date_time(self, start_date_time):
        self.start_date_time = start_date_time

    def set_end_date_time(self, end_date_time):
        self.end_date_time = end_date_time

    def get_id(self):
        return self.id

    def get_user_id(self):
        return self.user_id

    def get_transportation_mode(self):
        return self.transportation_mode

    def get_start_date_time(self):
        return self.start_date_time

    def get_end_date_time(self):
        return self.end_date_time
