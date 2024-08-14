# flickr
Central repo housing the code to organize and maintain the org's flickr photos

## Table of Contents

| File Name | Description | No. Lines |
|   ---     | -------     | -------   |
| `data_pull.py` | API Call | ~150 |
| `.env` | Template for Flickr API Credentials| 4|
| `data_pull.requirements.txt` | Libraries in virtual environment | 18 |


### `data_pull.py` Usage
To retrieve only geo-tagged images
```shell
$ python data_pull.py
```
To retrieve all images
```shell
$ python data_pull.py -a 
```
The above two commands will output verbose processing details to standard output as albums and album pages are fetched. A logging file will be generated to catch exceptions called `log_it_{time stamp}.log`. If `data_pull.py` runs successfully then a file called `data_pull_{time stamp}.p.json` will contain the data. 
### Environment Requirements
To install all the necessary dependencies 
```shell
$ pip install -r data_pull.requirements.txt
```
### API Credentials
Apply for API credentials at [flickr.com/services/apps/create/apply/](https://www.flickr.com/services/apps/create/apply/). With the acquired credentials fill the `.env` template. 
#### Flickr API Documentation
[flickr.com/services/api/](https://www.flickr.com/services/api/)

# TODO
- Data Report
- Form for title field
- Usability improvements on map
- Create presentation deck
- Have fun  ✅
