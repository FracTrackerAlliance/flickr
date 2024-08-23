import flickrapi, os, json, datetime, time
import pandas as pd
from dotenv import load_dotenv
from typing import Union, Dict, List
from threading import Thread

JSONVal = Union[str, int, 'JSONArray', 'JSONObject']
JSONArray = List[JSONVal]
JSONObject = Dict[str, JSONVal]

attr=[]
pic_ct = 0
GET_NON_GEO = False # defaults to getting only geo-tagged images

def get_data_t(flickr, a_id, extra_fields):
    pg_start, pg_end, per_page = 1, 100, 500
    while pg_end >= pg_start:
        photos = flickr.photosets.getPhotos(photoset_id=a_id, page=pg_start, per_page=500, extras=extra_fields)
        pg_end = photos['photoset']['pages']
        album_name = photos['photoset']['title']
        album_pic_ct = photos['photoset']['total']
        curr_pg_len =  len(photos['photoset']['photo'])
        print(f"Retrieving album: {album_name}.\n\t{album_pic_ct} image assets. \n\tCurrently on page {pg_start} with {curr_pg_len}") 
        for pic in photos['photoset']['photo']:
            if pic['media'] != 'photo': continue # skip videos
            if not GET_NON_GEO and ('latitude' not in pic or pic['latitude'] is None or pic['latitude'] == 0): continue # skip non-geo tagged
            # direct_link = pic.get('url_m', None)
            # if direct_link is None: 
            attr.append({
                'id': pic['id'],
                'album_name': album_name,
                'album_id':a_id,
                'title': pic['title'],
                'lat': pic['latitude'],
                'lon': pic['longitude'],
                'tags': pic['tags'],
                'last_update': pic['lastupdate'],
                'date_taken': pic['datetaken'],
                'date_upload': pic['dateupload'],
                'o_width': pic['o_width'],
                'o_height': pic['o_height'],
                'views': pic['views'],
                'src_url': pic.get('url_m', pic.get('url_sq', pic.get('url_o', None))),
                'link': f"https://www.flickr.com/photos/fractracker/{pic['id']}/in/album-{a_id}"
            })
            global pic_ct; pic_ct += 1
        pg_start += 1

def create_worker(flickr, album_ids):
    extra_fields ='date_upload, date_taken, owner_name, original_format, last_update, geo, tags, o_dims, views, media, url_m, url_o, url_s'
    thread_pool = []
    for a_id in album_ids: # thread worker for each album
        t =Thread(target=get_data_t, args=(flickr, a_id, extra_fields))
        t.start()
        thread_pool.append(t)
        time.sleep(.35)# rate limit, could be lower
    for i in thread_pool: i.join() # wait for all threads to finish

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
    create_worker(flickr, album_ids)
    print(f"Total number of images retrieved: {pic_ct}\nSaving data to flickr_data.csv")
    if GET_NON_GEO: print("Flag -a passed. Will retrieve non-geo tagged images.")
    else: print("Flag -a not passed. Will skip non-geo tagged images.")
    attr_df = pd.DataFrame(attr)
    attr_df.to_csv('flickr_data.csv', index=False)

if __name__ == "__main__":
    print(os.sys.argv)
    if '-a' in os.sys.argv:
        GET_NON_GEO = True
        print("Flag -a passed. Will retrieve non-geo tagged images.")
    else: print("Flag -a not passed. Will skip non-geo tagged images.")
    main()  
