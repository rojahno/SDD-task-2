class User:

    def __init__(self, id, has_label):
        self.id = id
        self.has_label = has_label

    def set_id(self, id):
        self.id = id

    def set_has_label(self, has_label):
        self.has_label = has_label

    def get_id(self):
        return self.id

    def get_has_label(self):
        return self.has_label
