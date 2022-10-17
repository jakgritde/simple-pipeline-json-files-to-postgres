#!/bin/bash
printf "Input path: "
read -r path
files=$(fdfind -e json . "$path")

declare -i count=0

for file in $files
do
    a=$(wc -l "$file" | awk -F ' ' '{print $1}')
    count=$((count + a + 1))
    # a is the number of \n characters from each file
    # but we need number of lines so we need to plus 1
done

echo "the number of records = ${count}"