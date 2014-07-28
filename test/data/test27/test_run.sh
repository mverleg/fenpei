#!/bin/sh

# executable file for test job
# (some test char: %)

echo "running job test27"
echo "let's do 27 steps" > result.txt

for i in `seq 1 27`;
do
    echo "item #$i" >> result.txt
    sleep 1;
done

echo "it is done!" >> result.txt
echo "finished test27"


