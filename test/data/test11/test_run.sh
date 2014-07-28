#!/bin/sh

# executable file for test job
# (some test char: %)

echo "running job test11"
echo "let's do 11 steps" > result.txt

for i in `seq 1 11`;
do
    echo "item #$i" >> result.txt
    sleep 1;
done

echo "it is done!" >> result.txt
echo "finished test11"


