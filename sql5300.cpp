#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"

/*
sqlshell
    - create db environment if doesn't exist --> go to initialize (DB_CREATE)
    - prompt SQL >
    - parse
    - function unparse
    - print 
exec
    ...
    - create table
    - select
*/


using namespace hsql;
using namespace std;

int main(int len, char* args[])
{
    if(len != 2)
    {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }

    // need to check db existance?
    char* directory = args[1];
    cout << "sql5300 BerkDB environment at" << directory << endl;

    DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
    // try catch?
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    _DB_ENV = &env;
   
    // prompt SQL
    while (true)
    {
        cout << "SQL> ";
        string query = "";
        cin >> query;
        cout << endl;

        // query with nothing
        if(query.length() == 0) 
            continue; 
        if(query == "quit")
            break:
        // test?

    }

    return EXIT_SUCCESS;
}

// expressionToString: symbols *, ., ?,  

// operatorToString: and, or, not

// columnDefToString: DOUBLE, INT, TEXT

// tableRefToString: join, cross

// create table

// select

// insert

// execute select/insert/create