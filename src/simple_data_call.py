import flickrapi, os, json, datetime, time
import pandas as pd
from dotenv import load_dotenv
from typing import Union, Dict, List
from threading import Thread

JSONVal = Union[str, int, 'JSONArray', 'JSONObject']
JSONArray = List[JSONVal]
JSONObject = Dict[str, JSONVal]

attr=[]


def get_data_t(flickr, a_id, extra_fields):
    pg_start, pg_end, per_page = 1, 100, 500
    while pg_end >= pg_start:
        photos = flickr.photosets.getPhotos(photoset_id=a_id, page=pg_start, per_page=500, extras=extra_fields)
        pg_end = photos['photoset']['pages']
        
        album_name = photos['photoset']['title']
        for pic in photos['photoset']['photo']:
            title = pic['title']
            lat, lon = pic['latitude'], pic['longitude']    
            tags = pic['tags']
            last_update = pic['lastupdate']
            date_taken = pic['datetaken']
            date_upload = pic['dateupload']
            pid = pic['id']
            o_width, o_height = pic['o_width'], pic['o_height']
            src_url = pic['url_o']
            views = pic['views']
            link = url = f"https://www.flickr.com/photos/fractracker/{pid}/in/album-{a_id}"

            record = {
                'id': pid,
                'title': title,
                'lat': lat,
                'lon': lon,
                'tags': tags,
                'last_update': last_update,
                'date_taken': date_taken,
                'date_upload': date_upload,
                'o_width': o_width,
                'o_height': o_height,
                'views': views,
                'src_url': src_url,
                'link': link
            }
            attr.append(record)
        pg_start += 1

def create_worker(flickr, album_ids):
    extra_fields ='date_upload, date_taken, owner_name, original_format, last_update, geo, tags, o_dims, views, media, url_o'
    thread_pool = []
    for a_id in album_ids:
        # thread worker for each album
        t =Thread(target=get_data_t, args=(flickr, a_id, extra_fields))
        t.start()
        thread_pool.append(t)
        time.sleep(.3)# rate limit, could be lower

    # wait for all threads to finish
    for i in thread_pool: i.join()

def main():

    print(f"Local folder defined env variables found?.... {load_dotenv()=}")


    secret = os.getenv('SECRET')
    key = os.getenv('KEY')
    USER_NAME = os.getenv('ORG_USER_NAME')

    flickr = flickrapi.FlickrAPI(key, secret, format='parsed-json')

    user_info : JSONObject = flickr.people.findByUsername(username=USER_NAME)

    user_id : str = user_info['user']['id']

    photosets : JSONObject = flickr.photosets.getList(user_id=user_id)

    album_ids : List[str] = [albumMetaData['id'] for albumMetaData in photosets['photosets']['photoset']]

#    album_ids=album_ids[:2]
    create_worker(flickr, album_ids)
    attr_df = pd.DataFrame(attr)
    attr_df.to_csv('flickr_data.csv', index=False)


if __name__ == "__main__":
    main()  
