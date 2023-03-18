class LocalSwarmMemory(object):
    def __init__(self, owner_bot_id):
        self.owner_bot_id = owner_bot_id

        self.contents = {}
        self.data_to_holder_id_map = {}

    def write(self, path_to_write, value_to_write):
        path_components = path_to_write.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                curr_dict[key] = value_to_write
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]
        self.update_data_holder(path_to_write, self.owner_bot_id)

    def read(self, path_to_read):
        path_components = path_to_read.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    return curr_dict[key]
                return None
            else:
                curr_dict = curr_dict[key]

    def delete(self, path_to_delete):
        path_components = path_to_delete.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    curr_dict.pop(key)
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]

        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    curr_dict.pop(key)
            else:
                curr_dict = curr_dict[key]

    def has_path(self, path):
        path_components = path.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if key not in curr_dict:
                return False
            curr_dict = curr_dict[key]
        return True

    def update_data_holder(self, path, data_holder_id):
        if (data_holder_id != self.owner_bot_id) and (self.has_path(path)):
            self.delete(path)

        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                curr_dict[key] = data_holder_id
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]

    def get_key_holder_ids(self, path):
        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if key not in curr_dict:
                return []
            curr_dict = curr_dict[key]
        if isinstance(curr_dict, dict):
            return list(set(self.get_nested_dict_values(curr_dict)))
        else:
            return [curr_dict]

    def get_nested_dict_values(self, dict_to_parse):
        for value in dict_to_parse.values():
            if isinstance(value, dict):
                yield from self.get_nested_dict_values(value)
            else:
                yield value

    def get_data_to_holder_id_map(self):
        return self.data_to_holder_id_map

    def get_contents(self):
        return self.contents
