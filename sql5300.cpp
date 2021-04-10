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



// TODO: need to complete the code for some other cases and implement convertOperatorToStr()
/**
 * Convert Expression to string
 * @expr expr   expression to be converted
 * @return      SQL version of *expr
 */
string convertExpressionToStr(const Expr *expr) {
    string res;

    switch (expr->type) {
        case kExprLiteralFloat:
            res += to_string(expr->fval);
            break;
        case kExprLiteralString:
            res += expr->name;
            break;
        case kExprLiteralInt:
            res += to_string(expr->ival);
            break;
        case kExprStar:
            res += "*";
            break;
        // TODO: not sure how to handle below case
//        case kExprPlaceholder:
//            res += "?";
//            break;
//        case kExprColumnRef:
         case kExprFunctionRef:
             res += string(expr->name) + "?" + expr->expr->name;
             break;
         case kExprOperator:
             res += convertOperatorToStr(expr);
             break;
         default:
            ret += "UNKNOWN EXPRESSION";
            break;
    }

    if (expr->alias != NULL) {
        res += " AS " + expr->alias;
    }

    return res;
}

// TODO: implement utility function: convertColInfoToStr()
/**
 * Execute a create statement
 *
 * @param stmt  a create statement to be executed
 * @returns     a string representation of the SQL statement
 */
string executeCreate(const CreateStatement *stmt) {
    string res = "CREATE TABLE ";

    if (stmt->type != CreateStatement::kTable) {
        return "Unsupported operation! Currently, only support create table";
    }

    // check whether the table already exists
    if (stmt->ifNotExists) {
        res += "IF NOT EXISTS ";
    }

    res += string(stmt->tableName) + " (";

    bool addComma = false;

    for (ColumnDefinition *col : *stmt->columns) {
        if (addComma) {
            res += ", ";
        }
        res += convertColInfoToStr(col);
        addComma = true;
    }

    res += ")";

    return res;
}

// TODO: implement utility function: convertExpressionToStr(), and convertTableRefInfoToStr()
/**
 * Execute a select statement
 *
 * @param stmt  a select statement to be executed
 * @returns     a string representation of the SQL statement
 */
string executeSelect(const SelectStatement *stmt) {
    string res = "SELECT ";
    bool addComma = false;

    for (Expr *expr : *stmt->selectList) {
        if (addComma) {
            res += ", ";
        }
        res += convertExpressionToStr(expr);
        addComma = true;
    }

    res += " FROM " + convertTableRefInfoToStr(stmt->fromTable);
    if (stmt->whereClause != NULL) {
        res += " WHERE " + convertExpressionToStr(stmt->whereClause);
    }

    return res;
}

/**
 * Execute a sql statement
 *
 * @param stmt  sql statement to be executed
 * @returns     a string representation of the SQL statement
 */
string execute(const SQLStatement *stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return executeSelect((const SelectStatement *) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement *) stmt);
        default:
            return "Not supported operation";
    }
}

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