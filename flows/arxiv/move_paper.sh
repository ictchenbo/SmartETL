#path="/data/users/chenbo/wangning/SmartETL/data/arxiv/task"
path="$1"

target="/data/users/chenbo/data/papers/arxiv"

cd $path

for filename in $(ls); do
   ym=$(echo "$filename" | cut -d '.' -f1)
   #if [ "$ym" -lt "2505" ]; then
     parent=$target/$ym
     mkdir -p $parent
     mv $filename $parent/$filename
     echo $filename moved
   #fi
done

