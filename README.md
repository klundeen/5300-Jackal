# 5300-Jackal
5300-Jackal's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2021

Usage (argument is database directory):
- For build:
```
5300-Jackal$ make
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "sql5300.o" "sql5300.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "SlottedPage.o" "SlottedPage.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "HeapFile.o" "HeapFile.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "HeapTable.o" "HeapTable.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "ParseTreeToString.o" "ParseTreeToString.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "SQLExec.o" "SQLExec.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "schema_tables.o" "schema_tables.cpp"
g++ -I/usr/local/db6/include -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb -I../sql-parser/src -o "storage_engine.o" "storage_engine.cpp"
g++ -L/usr/local/db6/lib -o sql5300 sql5300.o SlottedPage.o HeapFile.o HeapTable.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o -ldb_cxx -lsqlparser
5300-Jackal$ 
```
- For executing
```
5300-Jackal$ ./sql5300 <path to the data folder>
```
-- The data folder must be created before and can be specified in absolute or relative path. Ex: `./sql5300 /home/user/data` or `./sql5300 ./data`

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone3</code> has the instructor's attempt to complete the Milestone 3 assignment.
- <code>Milestone4_prep</code> has the instructor-provided files for Milestone4 (this was all actually in Milestone3, too, by mistake).
- <code>Milestone4</code> has our implementation of the indexing.
## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Handoff:
- Milestone 3 should be completely off of Professor Lundeens' implementation. Operations like `create table ...`, `drop table ...` should have some safety check to ensure that specified table exist.
- Milestone 4 should be fully working as well. However, there are some lost bytes according to the Valgrind report when dropping a table with Kevin's implementation. 

### Video and Demo:
https://seattleu.instructuremedia.com/embed/47c4feec-3c38-461a-baa0-979ed6418c0b

## Other:
Created a ".gitignore" file where we specifyied the "data" folder to make sure the db files created are not committed.

