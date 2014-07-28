#!/bin/sh

# executable file for test job
# (some test char: %%)

echo "running job %(name)s"
echo "let's do %(N)d steps" > result.txt

for i in `seq 1 %(N)d`;
do
    echo "item #$i" >> result.txt
    sleep 1;
done

echo "it is done!" >> result.txt
echo "finished %(name)s"


