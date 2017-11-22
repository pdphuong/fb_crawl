import requests, os, shutil, datetime, time, json, csv, gzip,argparse, sys, configparser, urllib
from retrying import retry
from collections import deque
from multiprocessing import Pool


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
def fname_TOO_LONG_FEED():
	return __create_dir_on_read_FILE__('./output/errors/too_long_feeds.csv')
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
		raise Exception('url:%s err:%s'%(url,resp))
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
URL_REACTIONS 	= 'https://graph.facebook.com%s?limit=15000&after=%s&fields=id,type' + '&%s'%token
URL_COMMENTS_L1	= 'https://graph.facebook.com%s?limit=1000&&after=%s&fields=id,from{id},message,reactions.limit(100){id,type},comments.limit(100){id,from{id},message,reactions.limit(100){id,type}}' + '&%s'%token
URL_COMMENTS_L2 = 'https://graph.facebook.com%s?limit=1000&after=%s&fields=id,from{id},message,reactions.limit(5000){id,type}' + '&%s'%token

def exhaust_fetch(url):
	return __exhaust_fetch__(url)

def __exhaust_fetch__(url):
	
	def custom_url(url_0):

		parts = urllib.parse.urlsplit(url_0)
		params = urllib.parse.parse_qs(parts.query)
		after = params['after'][0]
		fields = params['fields'][0]
		
		if parts.path.split('/')[-1] == 'reactions':
			return URL_REACTIONS%(parts.path,after)
		elif parts.path.split('/')[-1] == 'comments':
			if 'comments' in fields: #comments
				return URL_COMMENTS_L1%(parts.path,after)
			else:
				return URL_COMMENTS_L2%(parts.path,after)
		else:
			raise Exception('Invalid url:%s'%url_0)

	def fill_obj(obj):
		assert type(obj) is dict, 'Invalid object :%s'%str(obj)

		if 'paging' in obj and 'next' in obj['paging']:			
			
			next_url = custom_url(obj['paging']['next'])			
			cont = True

			while(cont):

				try:
					next_obj = getRequests(next_url)
					obj['data'].extend(next_obj['data'])
					if 'paging' in next_obj and 'next' in next_obj['paging']:
						next_url = custom_url(next_obj['paging']['next'])
					else:
						cont = False
				except Exception as e:
					if 'After Cursor specified exceeds the max limit supported by this endpoint' in str(e):						
						logger.warning('Too long feed. url:%s err:%s'%(url,str(e)))
						with open(fname_TOO_LONG_FEED(),'a') as f:
							csv.DictWriter(f,['url','err']).writerow({'url':url,'err':str(e)})
						cont = False
					else:
						raise e

		for k,v in obj.items():
		
			if type(v) is dict:
				fill_obj(v)
			elif type(v) is list:
				for sub_obj in v:
					fill_obj(sub_obj)

	top_obj = getRequests(url)
	fill_obj(top_obj)

	return top_obj

###############################

################################
# STEP 1: FETCH FEED HEADERS
################################
def fetch_headers(page, since, until = None):
	
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
		
		if until and (created_time > until):
			continue
		
		if created_time <= since:			
			break
		
		headers.append(feed)

	return headers
	
	if len(headers) == 0:
		return date2str(since)

	for i in range(0,len(headers),5):
		timestamp = headers[i]['created_time']		
		fname = os.path.join(dir_TODO_BODY(page),'%s_%05d.json'%(timestamp,i))
		json.dump(headers[i:i+5],open(fname,'w'))

	return headers[0]['created_time']

def fetch_headers_all_pages():

	fname_TODO = fname_TODO_HEADER()
	fname_TODO_tmp = fname_TODO + '_tmp'

	todos = json.load(open(fname_TODO,'r'))	
	nw_todos = []

	logger.info('Begin fetch headers...')

	for task in todos:

		page, since = task['page'], str2date(task['since'])
		
		logger.info('\t Fetch headers for %s since %s'%(page,task['since']))
		
		headers = fetch_headers(page,since)

		if len(headers) == 0:
			
			nw_since = date2str(since)
		else:

			for i in range(0,len(headers),5):
			
				timestamp = headers[i]['created_time']		
				fname = os.path.join(dir_TODO_BODY(page),'%s_%05d.json'%(timestamp,i))
				json.dump(headers[i:i+5],open(fname,'w'))

			nw_since = headers[0]['created_time']

		nw_todos.append({'page':page,'since':nw_since,'last_since':task['since']})
		
		logger.info('\t Done')

	json.dump(nw_todos,open(fname_TODO_tmp,'w'))
	os.rename(fname_TODO_tmp,fname_TODO)

#################################

#################################
### STEP 2: FETCH FEED DETAILS
#################################
# def fetch_body(page,feed_id):
# 	#For broad posts
# 	url = 'https://graph.facebook.com/v2.7/%s?fields=reactions.limit(1000){id,type},comments.limit(1000){reactions.limit(100){id,type},comments.limit(100){reactions.limit(100){id,type},message,from{id},id},message,from{id},id},id,created_time,description,message,message_tags,link&%s'%(feed_id,token)
# 	try:
# 		feed_body = exhaust_fetch(url)
# 		fname = fname_DONE_BODY(page,feed_id,feed_body['created_time'])		
# 		json.dump(feed_body,gzip.open(fname,'wt'))
# 	except Exception as e:
# 		err_msg = 'Error fetching page:%s - feed:%s'%(page,feed_id)
# 		logger.error(err_msg + '\n%s\n'%str(e))
# 		with open(fname_FAILED_FEED(),'a') as f:
# 			csv.DictWriter(f,['page','feed_id','url','err']).\
# 			writerow({'page':page,'feed_id':feed_id,'url':url,'err':str(e)})

# def feed_too_fresh(f_todo):
# 	now = datetime.datetime.now(datetime.timezone.utc)
# 	dt = str2date(f_todo[:24])
# 	diff =  now - dt
# 	return diff.days < 5
	
# def fetch_body_batches():
# 	logger.info('Begin fetch feed bodies...')
	
# 	pages = list_sub_dirs(dir_PAGES())	
# 	page_ftodos = []
# 	for page in pages:
# 		dir = dir_TODO_BODY(page)
# 		for f_todo in sorted(list_files(dir)):
# 			page_ftodos.append((page,f_todo))

# 	# In Parallel:
# 	with Pool(16) as p:
# 		p.map(__fetch_body_batches__, page_ftodos)

# def __fetch_body_batches__(page_ftodo):

# 	page,f_todo = page_ftodo

# 	if feed_too_fresh(f_todo):
# 		logger.info('\tPage %s File %s is too new..skip for now'%(page,f_todo))
# 		return

# 	dir = dir_TODO_BODY(page)
# 	logger.info('Fetching Page %s File %s'%(page,f_todo))
# 	f_done = os.path.join(dir_DONE_BODY(page),f_todo)
# 	f_todo = os.path.join(dir,f_todo)
# 	todos = json.load(open(f_todo,'r'))
# 	for todo in todos:
# 		logger.info('Fetching Page %s File %s Feed %s'%(page,f_todo,todo['id']))
# 		fetch_body(page,todo['id'])
# 		logger.info('DONE fetching Page %s File %s Feed %s'%(page,f_todo,todo['id']))
# 	os.rename(f_todo,f_done)

##########################
def fetch_body_all(pages = None):
	
	logger.info('Begin fetch feed bodies...')
	
	if pages is None:
		pages = list_sub_dirs(dir_PAGES())

	page_ftodos = []
	for page in pages:
		dir = dir_TODO_BODY(page)
		for f_todo in sorted(list_files(dir)):
			page_ftodos.append((page,f_todo))

	# In Parallel:
	with Pool(16) as p:
		p.map(__fetch_body_ftodo__, page_ftodos)

def fetch_body_ftodo(page, f_todo):
	
	__fetch_body_ftodo__((page,f_todo))

def __need_fetch_body__(f_todo):
	
	now = datetime.datetime.now(datetime.timezone.utc)
	dt = str2date(f_todo[:24])
	diff =  now - dt
	
	if diff.days < 5:
		return False, 'Too new'

	return True, None

def __fetch_body_ftodo__(page_ftodo):
	
	page, f_todo = page_ftodo

	need_fetch, reason = __need_fetch_body__(f_todo)

	if not need_fetch:
		logger.info('Page: %s File: %s is not fetched. Reason: %s'%(page,f_todo,reason))
		return

	dir = dir_TODO_BODY(page)
	logger.info('Fetching Page: %s File: %s'%(page, f_todo))
	
	f_done = os.path.join(dir_DONE_BODY(page), f_todo)
	f_todo = os.path.join(dir, f_todo)
	todos = json.load(open(f_todo,'r'))

	for todo in todos:
	
		feed_id = todo['id']
		logger.info('Fetching Page: %s Feed: %s'%(page,feed_id))

		try:

			body = fetch_body_single(feed_id)
			f_body = fname_DONE_BODY(page, feed_id, body['created_time'])

			json.dump(body,gzip.open(f_body,'wt'))
		except Exception as e:
			
			err_msg = 'Error fetching page:%s - feed:%s. Details: %s'%(page,feed_id, str(e))
			logger.error(err_msg)
			
			with open(fname_FAILED_FEED(),'a') as f:
				csv.DictWriter(f,['page','feed_id','url','err']).\
				writerow({'page':page,'feed_id':feed_id,'err':err_msg})

	logger.info('DONE fetching Page %s File %s Feed %s'%(page,f_todo,todo['id']))
	
	os.rename(f_todo,f_done)

def fetch_body_single(feed_id):
	#For broad posts
	#url = 'https://graph.facebook.com/v2.7/%s?fields=reactions.limit(1000){id,type},comments.limit(1000){reactions.limit(100){id,type},comments.limit(100){reactions.limit(100){id,type},message,from{id},id},message,from{id},id},id,created_time,description,message,message_tags,link&%s'%(feed_id,token)	
	url = 'https://graph.facebook.com/v2.7/%s?fields=reactions.limit(1000){id,type},comments.limit(50){reactions.limit(20){id,type},comments.limit(20){reactions.limit(20){id,type},message,from{id},id},message,from{id},id},id,created_time,description,message,message_tags,link&%s'%(feed_id,token)
	return 	exhaust_fetch(url)

def __main__():

	parser = argparse.ArgumentParser(prog='fb_crawler')

	which_parser = parser.add_subparsers(dest='which',help='crawl feed header or feed body')
	which_parser.required = True
	
	header_parser = which_parser.add_parser('header', help = 'Crawl feed headers(e.g. ids)')
	header_parser.add_argument('-p', '--page', type=str, default=None)
	header_parser.add_argument('-s', '--since', type=str, default=None)
	header_parser.add_argument('-u', '--until', type=str, default=None)

	body_parser = which_parser.add_parser('body', help = 'Crawl feed body/details')
	body_parser.add_argument('-p', '--page', type=str, default=None)
	body_parser.add_argument('-f', '--f-todo', type=str, default=None)
	body_parser.add_argument('-I', '--id', type=str, default=None)

	args = parser.parse_args()

	print(args)

	if args.which == 'header':
		
		if args.page is None:
		
			fetch_headers_all_pages()
		else:

			until = str2date(args.until) if args.until else datetime.datetime.now()
			since = str2date(args.since) if args.since \
						else until - datetime.timedelta(1)
			
			headers = fetch_headers(args.page, since, until)
			print(json.dumps(headers))

	elif args.which == 'body':
		# fetch all pages as normal

		if args.id:

			try:
				print(json.dumps(fetch_body_single(args.id)))
			except Exception as e:
				raise e
		elif args.page is None and args.f_todo is None:			

			fetch_body_all()
		elif args.page and args.f_todo is None:
			
			fetch_body_all([args.page])
		elif args.page and args.f_todo:

			path_f_todo = os.path.join(dir_TODO_BODY(args.page), args.f_todo)
			assert os.path.exists(path_f_todo), '%s does not exist'%path_f_todo
			
			fetch_body_ftodo(args.page, args.f_todo)
		else:
			raise Exception("Must choose one:\n\
							1. Fetch body for all pages\n\
							2. Fetch body for 1 page\n\
							3. Fetch body for 1 page + 1 f_todo file\n\
							4. Fetch body for 1 feed")

if __name__ == '__main__':
	__main__()
