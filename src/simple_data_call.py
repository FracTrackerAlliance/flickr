import flickrapi, os, json, datetime, time, sys, re
import pandas as pd
from dotenv import load_dotenv
from typing import Union, Dict, List
from threading import Thread
from logger import logger

JSONVal = Union[str, int, 'JSONArray', 'JSONObject']
JSONArray = List[JSONVal]
JSONObject = Dict[str, JSONVal]

attr=[]
pic_ct = 0
GEO = True # Get only geo coded images? Defaults response as Yes(True), therefore, --> only getting geo coded images

def get_data_t(flickr, a_id, extra_fields):
    pg_start, pg_end, per_page = 1, 100, 500
    while pg_end >= pg_start:
        try: 
            photos = flickr.photosets.getPhotos(photoset_id=a_id, page=pg_start, per_page=500, extras=extra_fields)
        except Exception as e:
            logger.error(f"Error: {e} for {a_id=}")
            time.sleep(2)
            logger.error(f"\n\nCaught and Error. Just Waited 2 Seconds and Trying Again. Retrying for {a_id=}")
            try: 
                photos = flickr.photosets.getPhotos(photoset_id=a_id, page=pg_start, per_page=500, extras=extra_fields)
            except Exception as e:
                logger.error(f"Error: {e} for {a_id=}")
                logger.error(f"\n\nSECOND Exception for {a_id=} Now Shutting the entire program down.")
                sys.exit(1)
        pg_end = photos['photoset']['pages']
        album_name = photos['photoset']['title']
        album_pic_ct = photos['photoset']['total']
        curr_pg_len =  len(photos['photoset']['photo'])
        logger.info(f"Retrieving album: {album_name}.\n\t{album_pic_ct} image assets. \n\tCurrently on page {pg_start} with {curr_pg_len}") 
        for pic in photos['photoset']['photo']:
            if pic['media'] != 'photo': continue # skip videos
            lat = pic.get('latitude')
            lon = pic.get('longitude')
            if GEO:
                try:
                    lat = float(lat) ; lon = float(lon)
                    if lat == 0 or lon == 0: continue
                except Exception as e:
                    logger.error(f"Error: {e} for {pic['id']=}")
                    continue  
            record = {
                'Photo_ID': pic.get('id'),
                'Album_Name': album_name,
                'Album_ID':a_id,
                'Title': pic.get('title'),
                'Latitude': pic.get('latitude'),
                'Longitude': pic.get('longitude'),
                'Tags': pic.get('tags'),
                'Last_Update': pic.get('lastupdate'),
                'Date_Taken': pic.get('datetaken'),
                'Date_Upload': pic.get('dateupload'),
                'o_width': pic.get('o_width'),
                'o_height': pic.get('o_height'),
                'Views': pic.get('views'),
                'Source_Image_URL': pic.get('url_m', pic.get('url_sq', pic.get('url_o'))),
                'Link': f"https://www.flickr.com/photos/fractracker/{pic['id']}/in/album-{a_id}"
            }
            # check if None is returned for any of the fields
            for k, v in record.items():
                if v is None: 
                    logger.debug(f"Missing key: {k} for {record['id']}")
            attr.append(record)
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
    logger.info(f"Local folder defined env variables found?.... {load_dotenv()=}")
    secret = os.getenv('SECRET')
    key = os.getenv('KEY')
    USER_NAME = os.getenv('ORG_USER_NAME')
    flickr = flickrapi.FlickrAPI(key, secret, format='parsed-json')
    user_info : JSONObject = flickr.people.findByUsername(username=USER_NAME)
    user_id : str = user_info['user']['id']
    photosets : JSONObject = flickr.photosets.getList(user_id=user_id)
    album_ids : List[str] = [albumMetaData['id'] for albumMetaData in photosets['photosets']['photoset']]
    create_worker(flickr, album_ids)
    raw_data = 'x_raw_data.csv'
    logger.info(f"Total number of images retrieved: {pic_ct}\nSaving data to {raw_data}")
    attr_df = pd.DataFrame(attr)
    attr_df.to_csv(raw_data, index=False)
    # gc realease attr_df
    
    msg = f"Files created: \n\t{raw_data} - least processing"
    logger.info(msg)
    ## Clean up the data
    ## Grouping
    logger.info(f"Raw data shape: {attr_df.shape}")    
    # Drop duplicate images By way of groupby. The same image can be in multiple albums
    msg = f"Removing duplicate images. \n\tOriginal shape: {attr_df.shape}\n\tUnique images: {attr_df['Photo_ID'].nunique()}"
    logger.info(msg)    
    grouped_df = attr_df.groupby('Photo_ID')[['Album_Name','Tags']].agg(list).reset_index()
    # piece together the album names
    attr_df = attr_df.drop(['Album_Name', 'Tags'], axis=1) # these columns are now in grouped_df
    attr_df = attr_df.drop_duplicates(subset='Photo_ID', keep='first')  
    album_grp_df = pd.merge(grouped_df, attr_df, on='Photo_ID', how='left')
    logger.info(f"Shape after removing duplicates: {album_grp_df.shape}\nNew .csv file: flickr_data_album_grp.csv")
    album_grp_data = 'album_grp.csv'
    album_grp_df.to_csv(album_grp_data, index=False)
    
    ## Add geo data
    # Add county and state columns from the full_geo_store.csv. merge on id
    logger.info(f"Shape before merging with geo data:\n\t{album_grp_df.shape}")  
    geo_df = pd.read_csv('full_geo_store.csv')
    # as a double, convert both merges to same type int64
    geo_df['id'] = geo_df['id'].astype('int64')
    album_grp_df['Photo_ID'] = album_grp_df['Photo_ID'].astype('int64')
    album_grp_geo_df = pd.merge(album_grp_df, geo_df, left_on='Photo_ID', right_on='id', how='left')
    album_grp_geo_df.drop(columns=['id','lat','lon'], inplace=True)
    logger.info(f"Shape after merging with geo data: \n\t{album_grp_geo_df.shape}\n\tNew .csv file: flickr_data_album_grp_geo.csv") 
    f='x_flickr_data_album_grp_geo.csv'
    album_grp_geo_df.to_csv(f, index=False)
    
    ## Clean Title 
    
    # simple split
    ts = album_grp_geo_df.loc[:, 'Title'].map(lambda x: x.split('_')).copy(deep=True)
    album_grp_geo_df['ts'] = ts   

    # All are before the first underscore
    pic_taker = album_grp_geo_df.loc[:, 'ts'].map(lambda x: x[0]).copy(deep=True)
    album_grp_geo_df['Photographer'] = pic_taker

    # Title w/o photographer
    workTitle = album_grp_geo_df.loc[:, 'ts'].map(lambda x: "_".join(x[1:])).copy(deep=True)   # make a copy of the series   
    album_grp_geo_df['workTitle'] = workTitle 

    # remove file extension. All Titles that have a period, the period and everything after it is removed
    trimFileExt = album_grp_geo_df.loc[:, 'workTitle'].map(lambda x: x.split('.')[0]).copy(deep=True)   
    album_grp_geo_df['workTitle'] = trimFileExt

    # extract trailing 4 number or 4 number followed by a number or two in parentheses
    remove_trailing_yr = album_grp_geo_df.loc[:, 'workTitle'].map(lambda x: re.sub(r"([\d]{4})([(][\d]{1,2}[)])?$", "", x)).copy(deep=True)   # make a copy of the series
    album_grp_geo_df['workTitle'] = remove_trailing_yr

    # 
    mission_descript= album_grp_geo_df['workTitle'] .map(lambda x:x.split('_')[0]).copy(deep=True)
    album_grp_geo_df['Mission_Description'] = mission_descript

    # remove columns that are no longer needed
    album_grp_geo_df.drop(columns=['ts','workTitle'], inplace=True)
    final_final = 'flickr_data_album_grp_geo_final.csv'
    album_grp_geo_df.to_csv(final_final, index=False)
    msg = f"Final file created: \n\t{final_final}"
    logger.info(msg)


if __name__ == "__main__":
    runConfig = None
    if '-a' in os.sys.argv:
        GEO = False # Get all images
        runConfig="Flag -a passed. Will retrieve ALL images."
    else: runConfig = "Flag -a not passed. Will skip non-geo tagged images to only return geo coded images."
    logger.info(f"Program runnings with {os.sys.argv} and Configs as {runConfig}")
    main()  
    logger.info(f"Program ran with {os.sys.argv} and Config was {runConfig}")
