#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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



using namespace std;

int main(int len, char* args[])
{
    if(len != 2)
    {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }


   
    return EXIT_SUCCESS;
}