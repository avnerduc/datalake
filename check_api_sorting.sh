curl -s 'http://localhost:9900/upstream/vehicle_messages?amount=50000' -o api_sort_check.json
jq -r '.[].timestamp' api_sort_check.json | awk 'NR==1{p=$1;asc=1;desc=1;next}{if($1<p)asc=0; if($1>p)desc=0; p=$1}END{print asc?"sorted_asc":desc?"sorted_desc":"UNSORTED"}'

