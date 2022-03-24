from pennsieve import Pennsieve
import pennsieve
from app.config import Config

class BFWorker(object):
  def __init__(self, id):
    self.bf = Pennsieve(api_token=Config.PENNSIEVE_API_TOKEN, api_secret=Config.PENNSIEVE_API_SECRET)


  def getCollectionAndMetaFromPackageId(self, packageId):
    pkg = self.bf.get(packageId)
    if type(pkg) is pennsieve.DataPackage:
      colId = pkg.parent
      col = self.bf.get(colId)
      items = col.items
      for item in items:
        if packageId == item.id:
          return [colId, item.name]
    return None

  def getURLFromCollectionIdAndFileName(self, collectionId, fileName):
    col = self.bf.get(collectionId)
    if type(col) is pennsieve.Collection:
      items = col.items
      for item in items:
        if fileName == item.name:
          pkg = item
          try:
            bfFile = pkg.files[0]
            url = bfFile.url
            return url
          except:
            return None
    return None

  def getUrlfromPackageId(self, packageId):
    pId = packageId
    if ('N:' not in packageId):
      pId = 'N:' + packageId
    pk = self.bf.get(pId)
    return pk.files[0].url

  def getImagefromPackageId(self, packageId):
    pId = packageId
    if ('N:' not in packageId):
      pId = 'N:' + packageId
    pk = self.bf.get(pId)
    # resp = requests.get(pk.files[0].url)
    return pk.files[0].url if pk is not None else ''

  def getURLFromDatasetIdAndFilePath(self, datasetId, filePath):
      fileArray = filePath.split('/')
      items = self.bf.get_dataset(datasetId).items
      count = 0
      while type(items) is list:
          item = items[count]
          for fileName in fileArray:
              if fileName == item.name:
                  if type(item) is pennsieve.Collection:
                      items = item.items
                      count = -1
                      continue
                  else:
                      try:
                          return item.files[0].url
                      except:
                          return None
          count += 1
      return None
