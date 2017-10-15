import requests, os, shutil, datetime, time, json, csv, gzip,argparse, sys, configparser
from retrying import retry
from collections import deque
from multiprocessing.dummy import Pool


#################################################################
###	FILE UTILS
#################################################################
def __created_dir_on_read__(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)
	return dir
def __create_dir_on_read_FILE__(fname):
	dir = os.path.dirname(fname)
	__created_dir_on_read__(dir)
	return fname

def list_sub_dirs(dir,full_path=False):
	dirnames = [e for e in os.listdir(dir) if not os.path.isfile(os.path.join(dir,e))]
	if full_path:
		dirnames = map(lambda s: os.path.join(dir,s), dirnames)
	return dirnames

def list_files(dir,full_path=False):
	filenames = [e for e in os.listdir(dir) if os.path.isfile(os.path.join(dir,e))]
	if full_path:
		filenames = map(lambda s: os.path.join(dir,s), filenames)
	return filenames

def fname_LOG():
	return __create_dir_on_read_FILE__('./output/logs/log')
def fname_FAILED_FEED():
	return __create_dir_on_read_FILE__('./output/errors/feeds_failed_to_fetch.csv')
def fname_TODO_HEADER():
	return __create_dir_on_read_FILE__('./output/todo_header.json')
def dir_DONE_HEADER(page):
	return __created_dir_on_read__('./output/pages/%s/done_header/'%page)
def dir_TODO_BODY(page):
	return __created_dir_on_read__('./output/pages/%s/todo_body/'%page)	
def dir_PAGES():
	return __created_dir_on_read__('./output/pages/')
def dir_DONE_BODY(page):
	return __created_dir_on_read__('./output/pages/%s/done_body/'%page)
def dir_BODY(page):
	return __created_dir_on_read__('./output/pages/%s/feeds/'%page)
def fname_DONE_BODY(page,feed_id,created_time):
	return __create_dir_on_read_FILE__(os.path.join(dir_BODY(page),created_time + '_' + feed_id +'.json.gz'))
#################################################################
### END OF FILE UTILS
#################################################################

import logging 
logging.basicConfig(filename=fname_LOG(),level=logging.INFO,
	format='%(asctime)s.%(msecs)03d %(levelname)s %(funcName)s: %(message)s', 
	datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger('fb_crawl')

config = configparser.ConfigParser()
config.read('./private_conf.conf')
app_id = config['APP']['app_id'] 
app_secret = config['APP']['app_secret']
token = 'access_token=' + app_id + '|' + app_secret

def str2date(str):
	return datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S%z")
def date2str(date):
	return date.strftime("%Y-%m-%dT%H:%M:%S%z")

###############################

def getRequests(url):
	resp = __getRequests__(url)
	if 'error' in resp:
		raise Exception(resp)
	else:
		return resp

@retry(wait_random_min=1000, wait_random_max=10000)#Wait randomly 1sec to 10sec
def __getRequests__(url):
	try:
		requests_result = requests.get(url, headers={'Connection':'close'}).json()
		time.sleep(0.01)
		return requests_result
	except Exception as e:
		logger.error('Error in handling ' + url + str(e))
		raise e
###############################
#@retry(wait_random_min=10000, wait_random_max=30000,stop_max_attempt_number=2)#Wait randomly 10sec to 30sec
def exhaust_fetch(url):
	return __exhaust_fetch__(url)

def __exhaust_fetch__(url):

	next_pages = deque()
	fetched_segments = list()

	def get_next_paging(obj):
		assert type(obj) is dict

		if 'data' in obj \
			and 'paging' in obj \
			and 'next' in obj['paging']:

			next_url = obj['paging']['next']
			append_point = obj['data']
			next_pages.append((append_point,next_url))

			for e in obj['data']:
				get_next_paging(e)
		
		for k,v in obj.items():
			if k in ['data','paging']: continue
			if type(v) is dict:
				get_next_paging(v)

	top_resp = getRequests(url)
	get_next_paging(top_resp)

	while(len(next_pages) > 0):
		append_point, next_url = next_pages.pop()
		sub_resp = getRequests(next_url)
		assert 'data' in sub_resp
		fetched_segments.append((append_point,sub_resp))
		get_next_paging(sub_resp)

	while(len(fetched_segments) > 0):
		append_point, sub_resp = fetched_segments.pop()
		append_point.extend(sub_resp['data'])

	return top_resp
###############################

################################
# STEP 1: FETCH FEED HEADERS
################################
def fetch_headers(page, since):
	
	def iter_fetch_headers(page):		
		url = 'https://graph.facebook.com/v2.7/%s?fields=feed.limit(100){created_time,message,message_tags,description,link}&%s'%(page,token)		
		while(True):
			feeds = getRequests(url)
			feeds = feeds['feed'] if 'feed' in feeds else feeds
			if 'data' not in feeds:
				logger.warning(log_prefix + "'data' not in response. URL:%s"%url)
				break		
			for feed in feeds['data']:
				yield feed

			if 'paging' in feeds and 'next' in feeds['paging']:
				url = feeds['paging']['next']
			else:				
				break

	headers = []
	for feed in iter_fetch_headers(page):
		created_time = str2date(feed['created_time'])
		if created_time <= since:			
			break
		headers.append(feed)
	
	if len(headers) > 0:		
		timestamp = headers[0]['created_time']
		fname = os.path.join(dir_TODO_BODY(page),timestamp + '.json')
		json.dump(headers,open(fname,'w'))
		return timestamp
	else:
		return date2str(since)

def fetch_headers_all_pages():

	fname_TODO = fname_TODO_HEADER()
	fname_TODO_tmp = fname_TODO + '_tmp'

	todos = json.load(open(fname_TODO,'r'))	
	nw_todos = []
	logger.info('Begin fetch headers...')
	for task in todos:
		page, since = task['page'], str2date(task['since'])
		logger.info('\t Fetch headers for %s since %s'%(page,task['since']))
		until = fetch_headers(page,since)
		nw_todos.append({'page':page,'since':until,'last_since':task['since']})
		logger.info('\t Done')

	json.dump(nw_todos,open(fname_TODO_tmp,'w'))	
	os.rename(fname_TODO_tmp,fname_TODO)
#################################

#################################
### STEP 2: FETCH FEED DETAILS
#################################
def fetch_body(page,feed_id):
	#For broad posts
	url = 'https://graph.facebook.com/v2.7/%s?fields=reactions.limit(5000){id,type},comments.limit(2000){reactions.limit(1000){id,type},comments.limit(1000){reactions.limit(1000){id,type},message,from{id},id},message,from{id},id},id,created_time,description,message,message_tags,link&%s'%(feed_id,token)
	#For deep posts
	#url = 'https://graph.facebook.com/v2.7/%s?fields=reactions.limit(5000),comments.limit(200){reactions.limit(1000){id,type},comments.limit(1000){reactions.limit(1000){id,type},message,from{id},id},message,from{id},id},id,created_time,description,message,message_tags,link&%s'%(feed_id,token)
	try:
		feed_body = exhaust_fetch(url)
		fname = fname_DONE_BODY(page,feed_id,feed_body['created_time'])		
		json.dump(feed_body,gzip.open(fname,'wt'))
	except Exception as e:
		err_msg = 'Error fetching page:%s - feed:%s'%(page,feed_id)
		logger.error(err_msg + '\n%s\n'%str(e))
		with open(fname_FAILED_FEED(),'a') as f:
			csv.DictWriter(f,['page','feed_id','url']).writerow({'page':page,'feed_id':feed_id,'url':url})

def feed_too_fresh(f_todo):
	now = datetime.datetime.now(datetime.timezone.utc)
	dt = str2date(f_todo[:24])
	diff =  now - dt
	return diff.days < 5
	
def fetch_body_batches():
	logger.info('Begin fetch feed bodies...')
	
	pages = list_sub_dirs(dir_PAGES())	
	page_ftodos = []
	for page in pages:
		dir = dir_TODO_BODY(page)
		for f_todo in sorted(list_files(dir)):
			page_ftodos.append((page,f_todo))
	
	# In Parallel:
	with Pool(16) as p:
		p.map(__fetch_body_batches__, page_ftodos)


def __fetch_body_batches__(page_ftodo):

	page,f_todo = page_ftodo

	if feed_too_fresh(f_todo):
		logger.info('\tPage %s File %s is too new..skip for now'%(page,f_todo))
		return

	dir = dir_TODO_BODY(page)
	logger.info('Fetching Page %s File %s'%(page,f_todo))
	f_done = os.path.join(dir_DONE_BODY(page),f_todo)
	f_todo = os.path.join(dir,f_todo)
	todos = json.load(open(f_todo,'r'))
	for todo in todos:
		logger.info('Fetching Page %s File %s Feed %s'%(page,f_todo,todo['id']))
		fetch_body(page,todo['id'])
		logger.info('DONE fetching Page %s File %s Feed %s'%(page,f_todo,todo['id']))
	os.rename(f_todo,f_done)

def __main__():
	parser = argparse.ArgumentParser(prog='fb_crawler')

	token_group = parser.add_mutually_exclusive_group()
	token_group.add_argument('--token_file',action="store",type=str,help='Text file containing APP_ID + "|" + APP_SEC')
	token_group.add_argument('--token_string',action="store",help='String containing APP_ID + "|" + APP_SEC')

	subparsers = parser.add_subparsers(help='commands',dest='cmd')
	subparsers.required = True
	#crawl headers
	header_parser = subparsers.add_parser('header', help = 'Crawl feed headers(e.g. ids)')
	header_parser.set_defaults(func=fetch_headers_all_pages)
	#crawl feed body
	details_parser = subparsers.add_parser('detail', help = 'Crawl feed details')
	details_parser.set_defaults(func=fetch_body_batches)

	args = parser.parse_args()

	# if args.token_file:
	# 	with open(args.token_file,'r') as f:
	# 		token = f.readline()	
	# elif args.token_string:
	# 	token = args.token_string
	
	args.func()

if __name__ == '__main__':
	__main__()
