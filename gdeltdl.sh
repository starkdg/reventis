#!/bin/bash


gdelt_url="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

key="gdelt"
content_regexp="gdeltv2/2015.*.export.CSV.zip"

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
	
	./loadgdelt ${key} /tmp/${csv_file_name} > /dev/null 2>&1
	
	rm -rf /tmp/${compressed_file_name} /tmp/${csv_file_name}
	
done

echo "no. files processed" "${#content_components[@]}"






