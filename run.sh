architecture=$1
metric=$2
number_of_messages=$3
number_of_clients=$4
left=$5
right=$6
step=$7

output_name="server_$metric._$architecture.txt"
echo "$1" "$2" "$3" "$4" "$8" "$9" > "$output_name"

if [ "$metric" = "N" ]; then
    number_of_threads=$8;
    time_between=$9
    for (( i=left; i<right; i=i+step )); do
        echo -n "$i " >> "$output_name"
        echo "done $i $left $right"
        ./gradlew -q :run --args="$architecture $number_of_messages $number_of_clients $i $number_of_threads $time_between" >> "$output_name"
    done
fi

if [ "$metric" = "M" ]; then
    number_of_elements=$8;
    time_between=$9
    for (( i=left; i<right; i=i+step )); do
        echo -n "$i " >> "$output_name"
        echo "done $i $left $right"
        ./gradlew -q :run --args="$architecture $number_of_messages $number_of_clients $number_of_elements $i $time_between" >> "$output_name"
    done
fi

if [ "$metric" = "delta" ]; then
    number_of_elements=$8;
    number_of_threads=$9
    for (( i=left; i<right; i=i+step )); do
        echo -n "$i " >> "$output_name"
        echo "done $i $left $right"
        ./gradlew -q :run --args="$architecture $number_of_messages $number_of_clients $number_of_elements $number_of_threads $i" >> "$output_name"
    done
fi