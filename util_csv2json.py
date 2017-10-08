import csv,json
import sys,os
import datetime

def read_csv(infile,headers):
	content = []
	with open(infile,'r') as f:
		reader = csv.DictReader(f,fieldnames = headers)
		content = [row for row in reader]
	return content

# args = sys.argv
# assert len(args) >=3, 'Usage: csv2json infile outfile field1 field2 ....'
# csv_content = read_csv(args[1],args[3:])
# json.dump(content,open(args[2],'w'))

# headers = ['id','created_time','link','description','message','message_tags']
# batch_size = 10
# for dir,_,files in os.walk('../v_1/result/'):
# 	if 'feed_ids.csv' in files:
# 		infile = os.path.join(dir,'feed_ids.csv')
# 		page = dir.split('/')[-1]				
# 		outfile = './output/pages/'+ page + '/todo_body/1900-01-01T00:00:00+0000_%05d.json'
# 		csv_content = read_csv(infile,headers)

# 		for i in range(0,len(csv_content),batch_size):
# 			fname = outfile%i
# 			print('dump to file: %s'%fname)
# 			json.dump(csv_content[i:i+batch_size],open(fname,'w'))

#Remove all v1_legacy.json files
# for dir,_,files in os.walk('../v_1/result/'):
# 	if 'feed_ids.csv' in files:		
# 		page = dir.split('/')[-1]				
# 		src_name = './output/pages/'+ page + '/todo_body/v1_legacy.json'				
# 		if os.path.exists(src_name):
# 			print('Remove file:%s'%src_name)
# 			os.remove(src_name)

