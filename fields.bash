fields=$(cut -f1 -d, "$2" | sort | uniq | paste -sd ",")
cat $1 | jq "[.[] | {$fields}]"
