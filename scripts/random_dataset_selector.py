import datetime
import random
from app.metrics.algolia import get_all_dataset_ids, init_algolia_client

class RandomDatasetSelector(object):
    def __init__(self):
        self.last_used_time = datetime.datetime.now()
        self.available_dataset_ids = []
        self.random_id = -1

    def get_random_dataset_id(self, timeDeltaHours):
        timeDeltaDays = timeDeltaHours / 24
        now = datetime.datetime.now()
        # If a request is received in a window of time that is shorter than the time delta just return the same id, otherwise update the id
        if (now - self.last_used_time) < datetime.timedelta(days=timeDeltaDays) and self.random_id != -1:
            return self.random_id
        # reset the list of ids if we have iterated through all of them already
        if len(self.available_dataset_ids) == 0:
            self.available_dataset_ids = self.get_all_dataset_ids()
        random_index = random.randint(0, len(self.available_dataset_ids)-1)
        self.random_id = self.available_dataset_ids.pop(random_index)
        self.last_used_time = now
        return self.random_id

    def get_all_dataset_ids(self):
      algolia = init_algolia_client()
      return get_all_dataset_ids(algolia)
