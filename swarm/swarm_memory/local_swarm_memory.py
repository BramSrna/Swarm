class LocalSwarmMemory(object):
    def __init__(self, owner_bot_id):
        self.owner_bot_id = owner_bot_id

        self.contents = {}
        self.data_to_holder_id_map = {}

    def write(self, new_key_to_write, new_value_to_write, data_type):
        self.contents[new_key_to_write] = {
            "DATA_TYPE": data_type,
            "VALUE": new_value_to_write
        }
        self.update_data_holder(new_key_to_write, self.owner_bot_id, data_type)

    def read(self, key_to_read):
        if key_to_read in self.contents:
            return self.contents[key_to_read]
        return None

    def delete(self, key_to_delete):
        if key_to_delete in self.contents:
            self.contents.pop(key_to_delete)
        if key_to_delete in self.data_to_holder_id_map:
            self.data_to_holder_id_map.pop(key_to_delete)

    def has_data_key(self, data_key):
        return data_key in self.contents

    def update_data_holder(self, data_key, data_holder_id, data_type):
        self.data_to_holder_id_map[data_key] = {
            "DATA_TYPE": data_type,
            "HOLDER_ID": data_holder_id
        }

    def get_data_holder_id(self, data_key):
        if data_key in self.data_to_holder_id_map:
            return self.data_to_holder_id_map[data_key]["HOLDER_ID"]
        return None

    def get_ids_of_contents_of_type(self, type_to_get):
        ids = []
        for data_key, data_info in self.contents.items():
            if data_info["DATA_TYPE"] == type_to_get:
                ids.append(data_key)
        for data_key, data_info in self.data_to_holder_id_map.items():
            if data_info["DATA_TYPE"] == type_to_get:
                ids.append(data_key)
        return ids
