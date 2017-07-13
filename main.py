import csv, time

# SETTINGS
filename 		= 'influx_rar_ojan/tags_ind4.txt'
measurement		= 'data'
db				= 'tags'
export_filename = 'export.txt'
delim			= '\t'

timestamp_input_format 	= '%d/%m/%Y %H:%M:%S'

timestamp_col	= 'timestamp'
field_col		= 'name,addr'
tag_col	 		= 'type,datatype,quality,value,alarm,severity,lo2,lo1,hi1,hi2,limlo2,limlo1,limhi1,limhi2,mlo2,mlo1,mhi1,mhi2,ket'
quoted_col		= 'name,addr,ket'
# ./settings

# Split the tag and field col
field_col 	= field_col.split(',')
tag_col 	= tag_col.split(',')
quoted_col 	= {x: True for x in quoted_col.split(',')}

# Start of the Exported file, create the required database
with open(export_filename, 'w') as f:
	f.write("\
# DDL\n\
CREATE DATABASE {0} \n\
\n\
# DML\n\
# CONTEXT-DATABASE: {0} \n\
".format(db))

# DML Part of XML 
f = open(filename, 'r')
for row in csv.DictReader(f, delimiter=delim):
	try:
		timestamp 	= int(time.mktime(time.strptime(row['timestamp'], timestamp_input_format))) 	# Parse The timestamp
		valid_tag_col = filter(lambda x: row[x] != '', tag_col)
		valid_field_col = filter(lambda x: row[x] != '', field_col)
		tag_str 	= ",".join(map(lambda x: '{}="{}"'.format(x, row[x]) if x in quoted_col else "{}={}".format(x, row[x]) , valid_tag_col))	# Get the tag string
		field_str 	= ",".join(map(lambda x: '{}="{}"'.format(x, row[x]) if x in quoted_col else "{}={}".format(x, row[x]) , valid_field_col))	# Get the field string
		export_row	= "{},{} {} {}\n".format(measurement, tag_str, field_str, timestamp)				# DML LINE
		with open(export_filename, 'a') as f:
			f.write(export_row)
	except Exception as e:
		print("Error: {}. Data: {}".format(e, row))