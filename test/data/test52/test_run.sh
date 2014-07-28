#!/bin/sh

# executable file for test job
# (some test char: %)

echo "running job test52"
echo "let's do 52 steps" > result.txt

for i in `seq 1 52`;
do
    echo "item #$i" >> result.txt
    sleep 1;
done

echo "it is done!" >> result.txt
echo "finished test52"


