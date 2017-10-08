import os, shutil, datetime

def listmap(f,l):
	return list(map(f,l))

def str2date(str):
	return datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S%z")
def date2str(date):
	return date.strftime("%Y-%m-%dT%H:%M:%S%z")

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