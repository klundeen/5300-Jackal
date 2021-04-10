#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

// expressionToString: symbols *, ., ?,  

// operatorToString: and, or, not

// columnDefToString: DOUBLE, INT, TEXT

// tableRefToString: join, cross

// create table

// select

// insert

// execute select/insert/create ^^^





using namespace std;
using namespace hsql;

// initialize the db environment as global variable
DbEnv *DB_ENV;

// TODO: implement utility function: convertColInfoToStr()
/**
 * Execute a create statement
 *
 * @param stmt  a create statement to be executed
 * @returns     a string representation of the SQL statement
 */
string execCreate(const CreateStatement *stmt) {
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
string execSelect(const SelectStatement *stmt) {
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
 * Execute a insert statement
 *
 * @param stmt  sql statement to be executed
 * @returns     a string representation of the SQL statement
 */
string execInsert(const InsertStatement* stmt)
{
    return "INSERT ";
}

/**
 * Execute a sql statement
 *
 * @param stmt  sql statement to be executed
 * @returns     a string representation of the SQL statement
 */
string exec(const SQLStatement *stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return executeSelect((const SelectStatement *) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement *) stmt);
        default:
            return "Not supported operation";
    }
}

/**
 * Convert the expression into SQL statement
 *
 * @param stmt  an expression 
 * @returns     an transcribed formatted SQL statement
 */

string exprToString(const Expr *expr)
{
    string s = "";
    switch(expr->type)
    {
        case kExprOperator:
            s += operatorExprToString(expr);
            break;
        case kExprColumnRef:
            if(expr->table != NULL)
            {
                s += string(expr->table) + ".";
            }
        case kExprLiteralString:
            s += expr->name;
            break;
        case kExprLiteralFloat:
            string floatStr = to_string(expr->fval);
            s += floatStr;
            break;
        case kExprFunctionRef:
            s += string(expr->name) + "?" + expr->expr->name;
            break;
        case kExprLiteralInt:
            string litStr = to_string(expr->ival);
            break;
        case kExprStar:
            s += "s";
            break;
        default:
            ret += "?NotRecognize?";
            break;
    }
    if(expr->alias != NULL)
    {
        s += string(" AS ") + expr->alias;
    }
    return s;
}

//and, or, not
string operatorExprToString(const Expr *expr)
{

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

