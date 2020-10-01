#!/bin/bash


gdelt_url="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

# key value to use to submit to redis-server
key="gdelt"

# Adjust this variable to download different time spans.
# Vary resolutions to year, month or day by including YYYY, YYYYMM or YYYYMMDD

#content_regexp="gdeltv2/YYYY[0-9].*.export.CSV.zip"
#content_regexp="gdelv2/YYYYYMM[0-9].*.export.CSV.zip"
#content_regexp="gdeltv2/YYYYMMDD[0-9].*.export.CSV.zip"
content_regexp="gdeltv2/2016[0-9].*.export.CSV.zip"

redis_server="localhost"
redis_port=6379

content=$(curl -v --silent ${gdelt_url} --stderr - | grep $content_regexp)

read -d "\n" -a content_components <<< "$content"

n_elements=${#content_components[@]}

for ((n=0;n<${n_elements};n=n+3)) ; do

	current_url="${content_components[n+2]}"

	IFS='/' read -a url_components <<< "$current_url"

	compressed_file_name="${url_components[4]}"

	IFS='.' read -a file_components <<< "$compressed_file_name"

	csv_file_name="${file_components[0]}.${file_components[1]}.${file_components[2]}"
	
	curl --silent $current_url > /tmp/${compressed_file_name}

	unzip -p /tmp/${compressed_file_name} ${csv_file_name} > /tmp/${csv_file_name}  

	echo ${current_url} " --> " ${csv_file_name}
	
	./loadgdelt -k ${key} -s ${redis_server} -p ${redis_port} -f /tmp/${csv_file_name} &>/dev/null
	rm -rf /tmp/${compressed_file_name} /tmp/${csv_file_name}
	
done

echo "no. files processed" "${#content_components[@]}"






