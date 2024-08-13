from __future__ import annotations
import cProfile
import pstats
import flickrapi, os, json, time, sys, logging
from flickrapi.exceptions import FlickrError
import pandas as pd
from dotenv import load_dotenv
from typing import Union, Dict, List
from threading import Thread, Event

# Get non geo tagged photos? 
NON_GEO = False
# set up logging with timestamp in each log entry
log_file = f'log_it_{time.strftime("%m-%d-%Y-%I:%M:%S%p")}.log'
logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m-%d-%Y %I:%M:%S %p'
)
# forward referenced types are strings 
JSONVal = Union[str, int, 'JSONArray', 'JSONObject']
JSONArray = List[JSONVal]
JSONObject = Dict[str, JSONVal]
# global variable to store the data
attributes=[]
# Shared event for signaling
stop_event = Event()

def g_info_t(pic,flickr,album_title, a_id):
    photo_id = pic['id']
    try:
        photo_info = flickr.photos.getInfo(photo_id=photo_id)['photo']
    except FlickrError as e:
        if 'Status code 500' in str(e):
            print(f"Encountered an error for photo_id {photo_id}  \nWaiting 2 seconds and then trying again\n")
            # write the error to a log file
            logging.exception(f"Encountered an error for photo_id {photo_id}: {e}. \n{"-"*50}\n")    
            # rate limit
            time.sleep(2)
            try:
                photo_info = flickr.photos.getInfo(photo_id=photo_id)['photo']
            except Exception as e:
                print(f"Multiple Errors lets stop")
                print(f"ending program")
                # log the e in the exception 
                logging.exception(f"Encountered an error for photo_id {photo_id}: {e}. \n{"-"*50}\n")    
                sys.exit(1)
        else:
            print(f"Encountered an error for photo_id {photo_id}  Skipping this photo. waiting 2 seconds and then trying again")
            logging.error(f"Encountered an error for photo_id {photo_id}: {e}. \n{"-"*50}\n")    
            time.sleep(2)
            try:
                photo_info = flickr.photos.getInfo(photo_id=photo_id)['photo']
            except Exception as e:
                print(f"Multiple Errors lets stop")
                print(f"ending program")
                stop_event.set()  # Signal other threads to stop
                sys.exit(1) # exit the program
    except Exception as e:
        logging.exception(f"UNKNOWN ISSUE: Encountered an error for photo_id {photo_id}: {e}. \n{"-"*50}\n")    
        print(f"ending program due to unknown issue")
        stop_event.set()  # Signal other threads to stop
        sys.exit(1) # exit the program

    # retrieve the photo information
    title = photo_info['title']['_content']
    description = photo_info['description']['_content']
    url = f"https://www.flickr.com/photos/fractracker/{photo_id}/in/album-{a_id}"
    dt = photo_info['dates']['taken']

    # this network call works like a rate limiter 
    sizes = flickr.photos.getSizes(photo_id=photo_id)
    # Extract the direct link to image (ContentURL, original size URL)
    content_url = None
    for size in sizes['sizes']['size']:
        if size['label'] == 'Original':
            content_url = size['source']
            break
    if content_url:
        photo_src = content_url
    else:
        print(f"Original size ContentURL not found for {photo_id=} and {url=}.")

    # extact location info if exists
    latitude = None
    longitude = None
    if 'location' in photo_info:
        location_info = photo_info['location']
        if 'latitude' in location_info and 'longitude' in location_info:
            latitude = location_info['latitude']
            longitude = location_info['longitude']

    photo_record = {'PhotoID': photo_id,'Title': title, 'Date_taken': dt,'Description': description,'URL': url,
                'Latitude': latitude,'Longitude': longitude,'AlbumID': a_id,'AlbumTitle': album_title,'Photo_src_URL': photo_src}

    # if NON_GEO is False then only get the geo tagged photos with complete location info
    if NON_GEO or ('location' in photo_info and 'latitude' in location_info and 'longitude' in location_info):
        attributes.append(photo_record)

def get_pic_info(photos, flickr, album_title, a_id):
    thread_pool = []
    for pic in photos['photoset']['photo']:
        # thread worker for each photo
        t =Thread(target=g_info_t, args=(pic,flickr,album_title, a_id))
        t.start()
        thread_pool.append(t)
    # wait for all threads to finish
    for i in thread_pool: i.join()

def album_pull(flickr, album_ids):
    album_ct, album_len = 1, len(album_ids)
    for a_id in album_ids:
        album_start = time.time()
        pg_start, per_page = 1, 500
        photos : JSONObject = flickr.photosets.getPhotos(photoset_id=a_id, page=pg_start, per_page=per_page)
        for pg in range(1, photos['photoset']['pages']+1):
            pg_start = time.time()
            if pg!=1: photos : JSONObject = flickr.photosets.getPhotos(photoset_id=a_id, page=pg, per_page=per_page)
            print(f"Processing album number {album_ct}/{album_len}" )
            print(f"  On {pg=}/{photos['photoset']['pages']} of album \"{photos['photoset']['title']}\" id {photos['photoset']['id']}")
            print(f"    On this page of the album there are {len(photos['photoset']['photo'])}")
            album_title = photos['photoset']['title']
            get_pic_info(photos, flickr, album_title, a_id)
            pg_end = time.time()
            pg_elapsed = pg_end - pg_start
            print(f'\tTime taken for this page: {pg_elapsed:.6f} seconds')

        album_ct += 1   
        album_end = time.time()
        album_elapsed = album_end - album_start
        print(f'Time taken for this album: {album_elapsed:.6f} seconds')
        print("-"*50)

def main():
    # load env variables from .env file
    print(f"Local folder defined env variables found?.... {load_dotenv()=}")
    secret : str = os.getenv('SECRET')
    key : str = os.getenv('KEY')
    USER_NAME : str = os.getenv('ORG_USER_NAME')
     
    # login in and get an instance of the flickr api with user information 
    flickr : flickrapi.FlickrAPI = flickrapi.FlickrAPI(key, secret, format='parsed-json')
    user_info : JSONObject = flickr.people.findByUsername(username=USER_NAME)
    user_id : str = user_info['user']['id']
    
    # get the list of albums
    photosets : JSONObject = flickr.photosets.getList(user_id=user_id)
    album_ids : List[str] = [albumMetaData['id'] for albumMetaData in photosets['photosets']['photoset']]
    
    # just get the last few albums
    album_ids = album_ids[-18:-2]

    with cProfile.Profile() as pr:
        # album by album call getPhotos. Then photo by photo call getInfo and getSizes
        album_pull(flickr, album_ids) 
    time_stamp = time.strftime("%m-%d-%Y-%I:%M:%S%p")
    if NON_GEO: fname = 'data_pull_non_geo' # got all photos
    else: fname = 'data_pull_only_geo' # got only geo tagged photos
    
    with open(f'{fname}_{time_stamp}.p.json', 'w') as file: json.dump(attributes, file, indent=4)

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    # stats.print_stats()
    profiling_site = f'What_to_Profile_{time_stamp}.prof'
    stats.dump_stats(filename=profiling_site)

if __name__ == "__main__": 
    start_entire = time.time()
    # if the -a flag is passed then set the NON_GEO flag to True. Default behavior only pulls geo tagged photos
    if len(sys.argv) > 1 and sys.argv[1] == '-a':
        NON_GEO = True
        print(f'Getting all photos geo and non-geo tagged') 
    else:
        print(f'Getting only geo tagged photos')
    main()
    end_entire = time.time()
    elapsed_entire = end_entire - start_entire
    in_min = elapsed_entire/60  
    print(f'Time taken for this entire script: {elapsed_entire:.6f} seconds')
    print(f'Time taken for this entire script: {in_min:.6f} minutes')
