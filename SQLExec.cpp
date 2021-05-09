/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception &e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    if (statement->indexColumns == NULL) {
        return new QueryResult("did not create index as index columns not specified");
    }
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    Identifier index_name = statement->indexName;
    Identifier index_type = statement->indexType;
        
    // check if table exists
    if (!checkIfTableExists(statement->tableName)) {
        throw DbRelationError("table '" + string(statement->tableName) + "' does not exist");
    }
    // check if index already exists
    if (checkIfIndexExists(statement->tableName, statement->indexName)) {
        throw DbRelationError("table '" + string(statement->tableName) + "' already has index with name '" + statement->indexName + "'");
    }
    // check if provided columns exist
    checkIfColumnsExists(statement->indexColumns, statement->tableName);

    // get index table and metdata
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    column_attributes = indexTable.get_column_attributes();
    column_names = indexTable.get_column_names();

    int seqNum = 1;
    Handles c_handles;
    try {
        // for provided index columns, create index items
        for (auto const &col : *statement->indexColumns) {
            ValueDict row;
            row["table_name"] = Value(table_name);
            row["index_name"] = Value(index_name);
            row["column_name"] = Value(col);
            row["seq_in_index"] = Value(seqNum);
            row["index_type"] = Value(index_type);
            row["is_unique"] = Value(false);
            c_handles.push_back(indexTable.insert(&row));
            seqNum++;
            // delete row;
        }
    } catch (exception &e) {
        // attempt to remove/revert from _index if anything goes wrong
        try {
            for (auto const &handle: c_handles)
                indexTable.del(handle);
        } catch (...) {}
        throw;
    }

    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    // ensure user is not dropping any metadata tables
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || table_name == Indices::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    // check if table does not exist
    if (!checkIfTableExists(statement->name)) {
        throw DbRelationError("table '" + string(statement->name) + "' does not exist");
    }
    
    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove all indices for the table
    ValueDict where;
    where["table_name"] = Value(statement->name);
    // get all the indices for the table
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles *handles = indexTable.select(&where);
    for (auto const &handle: *handles) {
        indexTable.del(handle); // expect only one row from select
    }
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    // check if table exists and return error if not
    if (!checkIfTableExists(statement->name)) {
        throw DbRelationError("table '" + string(statement->name) + "' does not exist");
    }
    // check if index exists and return error if not
    if (!checkIfIndexExists(statement->name, statement->indexName)) {
        throw DbRelationError("table '" + string(statement->name) + "' does not have index '" + statement->indexName + "'");
    }

    // setup the where condition to get the indices for the table
    ValueDict where;
    where["table_name"] = Value(statement->name);
    where["index_name"] = Value(statement->indexName);
    // get all matching indices for the table
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles *handles = indexTable.select(&where);
    for (auto const &handle: *handles) {
        indexTable.del(handle); // remove the index
    }
    delete handles;
    
    return new QueryResult("dropped index " + string(statement->indexName) + " in table '" + string(statement->name) + "'");  // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    Identifier table_name = string(statement->tableName);
    // get index table pointer
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);

    // setup column names for the return data
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // get data from index table for provided table
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *handles = indexTable.select(&where);
    u_long n = handles->size();
    
    // setup data to be returned
    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = indexTable.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {   
    if (!checkIfTableExists(statement->tableName)) {
        throw DbRelationError("table '" + string(statement->tableName) + "' does not exist");
    }
    
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

bool SQLExec::checkIfTableExists(const char* tableName) {
    // setup query to see if table exists
    ValueDict where;
    where["table_name"] = Value(tableName);
    Handles *handles = SQLExec::tables->select(&where);
    bool resp = handles->size() > 0;
    
    delete handles;

    return resp;
}

void SQLExec::checkIfColumnsExists(const std::vector<char*>* columns, const char* tableName) {
    // get column table pointer
    DbRelation &columnTable = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // setup column names for the table metdata
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    // get all the colums for the provided table
    ValueDict where;
    where["table_name"] = Value(tableName);
    Handles *handles = columnTable.select(&where);
    std::vector<Value> *tableColumnsFound = new std::vector<Value>();
    for (auto const &handle: *handles) {
        ValueDict *row = columnTable.project(handle, column_names);
        tableColumnsFound->push_back(row->at("column_name"));
    }

    // check if those columns exist and return exception if not
    for (auto const &col: *columns) {
        bool found = false;
        string strCol = string(col);
        for(const Value &tableCol: *tableColumnsFound) {
            if (strCol.compare(tableCol.s) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw SQLExecError("column '" + strCol + "' does not exist for table '" + string(tableName)+ "'");
        }
    }

    delete handles;
}

bool SQLExec::checkIfIndexExists(const char* tableName, const char* indexName) {
    // get pointer to the index table
    DbRelation &indexTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    
    // get all the indices for the provided table and index name
    ValueDict where;
    where["table_name"] = Value(tableName);
    where["index_name"] = Value(indexName);
    Handles *duplicateCheck = indexTable.select(&where);
    // check and return if more than one, delete any empty pointers
    bool resp = duplicateCheck->size() > 0;
    delete duplicateCheck;
    return resp;
}
