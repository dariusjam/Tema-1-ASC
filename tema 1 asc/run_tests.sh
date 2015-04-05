SRC=tema
TESTS=tests
OUT=out
STATS=stats

rm -f ${OUT} ${STATS}

time python ${SRC}/tester.py -f ${TESTS}/test1 -o ${OUT} -t 1; echo
time python ${SRC}/tester.py -f ${TESTS}/test2 -o ${OUT} -t 20; echo
time python ${SRC}/tester.py -f ${TESTS}/test3 -o ${OUT} -t 4; echo
time python ${SRC}/tester.py -f ${TESTS}/test4 -o ${OUT} -t 4; echo
time python ${SRC}/tester.py -f ${TESTS}/test5 -o ${OUT} -t 10; echo
time python ${SRC}/tester.py -f ${TESTS}/test6 -o ${OUT} -t 10; echo
time python ${SRC}/tester.py -f ${TESTS}/test7 -o ${OUT} -t 1; echo
time python ${SRC}/tester.py -f ${TESTS}/test8 -o ${OUT} -t 1; echo
time python ${SRC}/tester.py -f ${TESTS}/test9 -o ${OUT} -t 2; echo
time python ${SRC}/tester.py -f ${TESTS}/test10 -o ${OUT} -t 1; echo

echo ""
echo "-----------------------------------------------------------------------"
echo ""

if [ -f ${OUT} ]
then
    cat ${OUT}
else
    echo "Tests failed"
fi
