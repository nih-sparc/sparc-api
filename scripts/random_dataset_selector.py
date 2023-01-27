import datetime
import random
from app.metrics.algolia import get_all_dataset_ids, init_algolia_client

class RandomDatasetSelector(object):
    def __init__(self):
        self.last_used_time = datetime.datetime.now()
        self.available_dataset_ids = []
        # limited_available_ids are used if a subset of ids is to be used for random selection as opposed to all id's
        self.limited_available_ids = []
        self.limited_available_ids_set = False
        self.random_id = -1

    def get_random_dataset_id(self, timeDeltaHours, limitedDatasetIds):
        self.set_limited_dataset_ids(limitedDatasetIds)
        timeDeltaDays = float(timeDeltaHours) / 24
        now = datetime.datetime.now()
        # If a request is received in a window of time that is shorter than the time delta and the limited available ids was not just set then return the same id, otherwise update the id
        if (now - self.last_used_time) < datetime.timedelta(days=timeDeltaDays) and self.random_id != -1  and self.limited_available_ids_set is False:
            return self.random_id
        # reset the list of ids if we have iterated through all of them already or if the limited available ids list was just set
        if len(self.available_dataset_ids) == 0 or self.limited_available_ids_set is True:
            if (len(self.limited_available_ids) > 0):
              self.available_dataset_ids = self.limited_available_ids.copy()
            else:
              self.available_dataset_ids = self.get_all_dataset_ids()
            # reset it to false so that the next time it is called with the same subset then it does not reset available_dataset_ids
            self.limited_available_ids_set = False
        random_index = random.randint(0, len(self.available_dataset_ids)-1)
        self.random_id = self.available_dataset_ids.pop(random_index)
        self.last_used_time = now
        return self.random_id

    def get_all_dataset_ids(self):
      algolia = init_algolia_client()
      return get_all_dataset_ids(algolia)
    
    def set_limited_dataset_ids(self, limitedAvailableIds):
      # if setting to the same values (regardless of order and duplicates) then do nothing
      if (set(self.limited_available_ids) == set(limitedAvailableIds)):
        return
      else:
        self.limited_available_ids = limitedAvailableIds
        self.limited_available_ids_set = True
