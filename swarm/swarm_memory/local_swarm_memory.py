import copy
import collections


class LocalSwarmMemoryEntry(object):
    def __init__(self, inner_value):
        self.inner_value = inner_value

    def get_inner_value(self):
        return self.inner_value


class LocalSwarmMemory(object):
    def __init__(self, key_count_threshold):
        self.contents = {}

        self.max_paths = key_count_threshold

    def create(self, path_to_create, value):
        path_components = path_to_create.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                curr_dict[key] = LocalSwarmMemoryEntry(value)
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]

    def read(self, path_to_read):
        path_components = path_to_read.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    return self.unwrap(curr_dict[key])
                return None
            else:
                curr_dict = curr_dict[key]

    def update(self, path_to_update, new_value):
        path_components = path_to_update.split("/")
        curr_dict = self.contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                curr_dict[key] = LocalSwarmMemoryEntry(new_value)
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

    def get_contents(self):
        return self.unwrap(self.contents)

    def get_usage_percentage(self):
        def count(d):
            return sum([count(v) if isinstance(v, dict) else 1 for v in d.values()])

        return (float(count(self.contents)) / float(self.max_paths)) * 100

    def unwrap(self, value_to_unwrap):
        if isinstance(value_to_unwrap, dict):
            value_to_unwrap = copy.deepcopy(value_to_unwrap)
            for key, value in value_to_unwrap.items():
                if isinstance(value, dict):
                    value_to_unwrap[key] = self.unwrap(value)
                else:
                    value_to_unwrap[key] = value.get_inner_value()
            return value_to_unwrap
        else:
            return value_to_unwrap.get_inner_value()

    def is_full(self):
        return self.get_usage_percentage() == 100

    def flatten(self, dictionary, parent_key=False, separator='/'):
        items = []
        for key, value in dictionary.items():
            new_key = str(parent_key) + separator + key if parent_key else key
            if isinstance(value, collections.abc.MutableMapping):
                items.extend(self.flatten(value, new_key, separator).items())
            else:
                items.append((new_key, value))
        return dict(items)

    def get_child_paths_from_parent_path(self, parent_path):
        child_paths = []
        flat_contents = self.flatten(self.contents)
        for path, _ in flat_contents.items():
            if path.startswith(parent_path):
                child_paths.append(path)
        return child_paths
