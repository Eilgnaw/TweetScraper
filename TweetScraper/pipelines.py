import os, logging, json
from scrapy.utils.project import get_project_settings

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs


logger = logging.getLogger(__name__)
SETTINGS = get_project_settings()

class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''

    def __init__(self):
        self.saveTweetPath = SETTINGS['SAVE_TWEET_PATH']
        self.saveUserPath = SETTINGS['SAVE_USER_PATH']
        mkdirs(self.saveTweetPath) # ensure the path exists
        mkdirs(self.saveUserPath)


    def process_item(self, item, spider):
        
        if isinstance(item, Tweet):
            savePath = os.path.join(self.saveTweetPath, item['id_']+".txt")
            logger.info("LOG Tweet if:%s" %savePath)
            if os.path.isfile(savePath):
                #logger.info("LOG Tweet if:%s" %savePath)
                pass # simply skip existing items
                # logger.debug("skip tweet:%s"%item['id_'])
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                #logger.info("Update tweet:%s"%item['id_'])
            else:
                #logger.info("LOG Tweet else:%s" %savePath)
                self.save_to_file(item,savePath)
                #logger.info("Add tweet:%s" %item['id_'])
            #logger.info("LOG Tweet logger:%s" %savePath)

        elif isinstance(item, User):
            #logger.info("LOG Tweet User:%s" %item['id_'])
            savePath = os.path.join(self.saveUserPath, item['id_']+".txt")
            if os.path.isfile(savePath):
                pass # simply skip existing items
                # logger.debug("skip user:%s"%item['id_'])
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.debug("Update user:%s"%item['id_'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add user:%s" %item['id_'])

        else:
            logger.info("Item type is not recognized! type = %s" %type(item))


    def save_to_file(self, item, fname):
        logger.debug("save_to_file tweet:%s" %item['id_'])
        ''' input: 
                item - a dict like object
                fname - where to save
        '''
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(dict(item), f, ensure_ascii=False)
