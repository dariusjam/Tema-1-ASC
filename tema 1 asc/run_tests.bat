@echo off

set SRC=tema
set TESTS=tests
set OUT=out
set STATS=stats

if exist %OUT% (
    del %OUT%
)

if exist %STATS% (
    del %STATS%
)

python %SRC%/tester.py -f %TESTS%/test1 -o %OUT% -t 1
python %SRC%/tester.py -f %TESTS%/test2 -o %OUT% -t 20
python %SRC%/tester.py -f %TESTS%/test3 -o %OUT% -t 4
python %SRC%/tester.py -f %TESTS%/test4 -o %OUT% -t 4
python %SRC%/tester.py -f %TESTS%/test5 -o %OUT% -t 10
python %SRC%/tester.py -f %TESTS%/test6 -o %OUT% -t 10
python %SRC%/tester.py -f %TESTS%/test7 -o %OUT% -t 1
python %SRC%/tester.py -f %TESTS%/test8 -o %OUT% -t 1
python %SRC%/tester.py -f %TESTS%/test9 -o %OUT% -t 2
python %SRC%/tester.py -f %TESTS%/test10 -o %OUT% -t 1

echo ""
echo "-----------------------------------------------------------------------"
echo ""

if exist %OUT% (
    type %OUT%
) else (
    echo "Tests failed"
)

pause
