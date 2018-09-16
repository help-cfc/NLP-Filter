import json
import ast
import numpy as np

import redis
import time

from urlparse import urlparse

metadata = 0
all_array = []
good_array = np.array(["0"])
bad_array = np.array(["0"])

class AddressFilter():
    def __init__(self):
        #parsed_redis = urlparse("rediss://admin:BXMOYDVFTLTTEBYL@portal1093-30.bmix-dal-yp-5e2ce16c-768f-4318-8456-404bf0b99ba0.261504680.composedb.com:57369")
        self.redis = redis.StrictRedis.from_url("rediss://admin:BXMOYDVFTLTTEBYL@portal1093-30.bmix-dal-yp-5e2ce16c-768f-4318-8456-404bf0b99ba0.261504680.composedb.com:57369")
    
    def get_tweet(self, i):
        packed = self.redis.brpop(['queue:tweet'])
        #print("try: %i", i)
        if not packed:
            print(i)
            i = i + 1
            return self.get_tweet(i)
        return packed
    
    def get_body(self, item):
        body = ast.literal_eval(item[1])
        print("data!")
        return body
        
    def categorize_tweet(self, body):
        global metadata
        if 'user' not in body:
            print body
            metadata = metadata + 1
        else:
            all_array.append(body)
        """if not extract_address(body):
            np.append(bad_array, [body])
        else:
            np.append(good_array, [body])"""
    
class FilteredTweetPipe():
    def __init__(self):
        self.redis = redis.StrictRedis.from_url("rediss://admin:BGXUJOSJVOSHTTSK@portal1122-32.bmix-dal-yp-9ed0e99e-4d7f-42bc-bd29-68826c7e3ca3.261504680.composedb.com:57374")
    
    def sanitize_tweet(self, tweet):
        body = tweet
        name = body["user"]["name"]
        user_id = body["user"]["id"]
        text = body["text"]
        place = body["place"]
        
        return {"name": name, "user_id": user_id, "text": text, "place": place}
        
    def push_sanitized_data(self, data):
        self.redis.lpush("queue:tweet", data)
        
        
def main():
    i = 0
    j = 1
    filter = AddressFilter()
    #print(filter.redis.llen('queue:tweet'))
    while True:
        print j
        tweet = filter.get_tweet(i)
        body = filter.get_body(tweet)
        filter.categorize_tweet(body)
        if j % 100 == 0:
            pipe = FilteredTweetPipe()
            for x in all_array:
                data = pipe.sanitize_tweet(x)
                pipe.push_sanitized_data(data)
            all_array[:] = []
            j = 1
            print("Done!")
        j = j + 1
        time.sleep(1)
        #print j
if __name__ == '__main__':
    main()