class TrackPoint:

    def __init__(self, id, activity_id, lat, lon, altitude, data_days, data_time):
        self.id = id
        self.activity_id = activity_id
        self.lat = lat
        self.lon = lon
        self.altitude = altitude
        self.data_days = data_days
        self.data_time = data_time

    def set_id(self, id):
        self.id = id

    def set_activity_id(self, activity_id):
        self.activity_id = activity_id

    def set_lat(self, lat):
        self.lat = lat

    def set_lon(self, lon):
        self.lon = lon

    def set_altitude(self, altitude):
        self.altitude = altitude

    def set_data_days(self, data_days):
        self.data_days = data_days

    def set_data_time(self, data_time):
        self.data_time = data_time

    def get_id(self):
        return self.id

    def get_activity_id(self):
        return self.activity_id

    def get_lat(self):
        return self.lat

    def get_lon(self):
        return self.lon

    def get_altitude(self):
        return self.altitude

    def get_data_days(self):
        return self.data_days

    def get_data_time(self):
        return self.data_time