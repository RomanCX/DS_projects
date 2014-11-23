cd ../src
make
cd ../bin
sh start-all.sh
sleep 1s
sh putInput.sh
sleep 1s
sh run_example1.sh
sleep 1s
sh run_example2.sh
sleep 1s
sh stop-all.sh
