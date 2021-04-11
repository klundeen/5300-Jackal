#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

// initialize the db environment as global variable
DbEnv *DB_ENV;


// forward declare
string convertOperatorToStr(const Expr *expr);
string convertExpressionToStr(const Expr *expr);
string executeCreate(const CreateStatement *stmt);
string executeSelect(const SelectStatement *stmt);
string execute(const SQLStatement *stmt);
string tableReftoString(const TableRef *table);
string colDefToStr(const ColumnDefinition* cd);


// TODO: below method didn't consider Ternary operators. And for unary operators, it only considers NOT so far
/**
 * Convert operation expression to string
 * @param expr  operator expression to be converted
 * @return      SQL version of operator expression
 */
string convertOperatorToStr(const Expr *expr) {
    if (expr == NULL) {
        return "null";
    }

    string res;

    if (expr->opType == Expr::NOT)
        res += "NOT ";

    res += convertExpressionToStr(expr->expr) + " ";

    switch (expr->opType) {
        case Expr::SIMPLE_OP:
            res += expr->opChar;
            break;
        case Expr::NOT_EQUALS:
            res += "!=";
            break;
        case Expr::LESS_EQ:
            res += "<=";
            break;
        case Expr::GREATER_EQ:
            res += ">=";
            break;
        case Expr::LIKE:
            res += "LIKE";
            break;
        case Expr::NOT_LIKE:
            res += "NOT_LIKE";
            break;
        case Expr::AND:
            res += "AND";
            break;
        case Expr::OR:
            res += "OR";
            break;
        case Expr::IN:
            res += "IN";
            break;
        default:
            break;
    }

    if (expr->expr2 != NULL) {
        res += " " + convertExpressionToStr(expr->expr2);
    }

    return res;
}








// TODO: implement utility function: convertColInfoToStr()
/**
 * Execute a create statement
 *
 * @param stmt  a create statement to be executed
 * @return     a string representation of the SQL statement
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
 * @return     a string representation of the SQL statement
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
 * @return     a string representation of the SQL statement
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

/**
 * Convert table expression to sql statement
 * @param t  operator expression to be converted
 * @return      SQL version of operator expression
 */
string tableReftoString(const TableRef *t)
{
    String s = "";
    switch(t->type)
    {
        case kTableSelect:
            s += "kTableSelect";
            break;
        case kTableName:
            s += t->name;
            if(t->alias != NULL)
                s += string(" AS ") + t->alias;
            break;
        case kTableJoin:
            s += tableReftoString(t->join->left);
            switch(t->join->type)
            {
                case kJoinInner:
                    s += " JOIN "
                    break;
                case kJoinLeft:
                    s += "LEFT JOIN ";
                    break;
                case kJoinRight:
                    s += "RIGHT JOIN";
                    break;
                case kJoinOtter:
                case kJoinLeftOuter:
                case kJoinRightOuter:
                case kJoincross:
                case kJoinNatural:
                    s += " NATURAL JOIN ";
                    break;
            }
            s += tableReftoString(t->join->right);
            if (t->join->condition != NULL)
                s += " ON " + convertExpressionToStr(t->join->right);
            break;

        case kTableCrossProduct:
            bool comma = false;
            for(TableRef *tbr : *t->list)
            {
                if(comma)
                    s += ", ";
                s += tableReftoString(tbr);
                comma = true; 
            }
            break;

    }
    return s;
}

/**
 * Convert column expression to type
 * @param cd  column expression to be converted
 * @return      corresponding type
 */
string colDefToStr(const ColumnDefinition* cd)
{
    string s(col->name);
    switch (col->type)
    {
        case ColumnDefinition::INT:
            s += " INT";
            break;
        case ColumnDefinition::DOUBLE:
            s += " DOUBLE";
            break;
        case ColumnDefinition::TEXT:
            s += " TEXT";
            break;
        default:
            s += " ?TYPE?";
            break;
    }
    return s;
}

int main(int len, char* args[])
{
    if(len != 2)
    {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }

    // create a BerkDB environment
    char* directory = args[1];
    cout << "sql5300 BerkDB environment is at" << directory << endl;

    DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	env.open(directory, DB_CREATE | DB_INIT_MPOOL, 0);

    _DB_ENV = &env;
   
    // prompt SQL
    while (true)
    {
        cout << "SQL> ";
        string query = "";
        cin >> query;
        cout << endl;

        if(query.length() == 0) 
            continue; 
        if(query == "quit")
            break;

        // parse
        SQLParserResult *pr = SQLParser::parseSQLString(query);
        if(!pr->isValid())
        {
            cout << "INVALID SQL: " << query << endl;
            cout << "Please enter a valid SQL query!" << endl;
            delete pr;
            continue;
        }
        cout << "parse ok!" << endl;
        for(uint i = 0; i < pr->size(); i++)
        {
            cout << exec(pr->getStatement(i))<< endl;
        }
        delete pr;
    }

    return EXIT_SUCCESS;
}

