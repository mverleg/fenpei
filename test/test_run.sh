
# executable file for test job
# (some test char: %%)

echo "let's do %(N)d steps"

for i in `seq 1 %(N)d`;
do
    echo "item #$i";
    sleep 1;
done

echo "it is done!";


