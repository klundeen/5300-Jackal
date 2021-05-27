/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @author Hailey Dice
 * @author Lili Hao
 * @author Tong Xu
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"
#include "EvalPlan.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

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

ValueDict* SQLExec::get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names) {
    if(expr->type != kExprOperator)
        throw DbRelationError("Operator is not supported");
    ValueDict* rows = new ValueDict;
    switch(expr->opType) {
        case Expr::AND: {
            ValueDict* sub = get_where_conjunction(expr->expr, col_names);
            if (sub != nullptr){
                rows->insert(sub->begin(), sub->end());
            }
            sub = get_where_conjunction(expr->expr2, col_names);
            rows->insert(sub->begin(), sub->end());
            break;
        }
        case Expr::SIMPLE_OP: {
            if(expr->opChar != '=')
                throw DbRelationError("only equality predicates currently supported");
            Identifier col = expr->expr->name;
            if(find(col_names->begin(), col_names->end(), col) == col_names->end()){
                throw DbRelationError("unknown column '" + col + "'");
            }
            if(expr->expr2->type == kExprLiteralString)
                rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->name)));
            else if(expr->expr2->type == kExprLiteralInt)
                rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->ival)));
            else
                throw DbRelationError("Type is not supported");
            break;
        }
        default:
            throw DbRelationError("only support AND conjunctions");
    }
    return rows;
}

            
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier tbn = statement->tableName;
    DbRelation& table = SQLExec::tables->get_table(tbn);
    
    ColumnNames cln;
    ColumnAttributes cla;
    
    ValueDict row;
    
    unsigned int index = 0;
    
    if(statement->columns != nullptr){
        for (auto const column : *statement->columns){
            cln.push_back(column);
        }
    }
    else{
        for (auto const column: table.get_column_names()){
            cln.push_back(column);
        }
    }
    
    for (auto const& column : *statement->values){
        switch(column->type){
            case kExprLiteralString:
                row[cln[index]] = Value(column->name);
                index++;
                break;
            case kExprLiteralInt:
                row[cln[index]] = Value(column->ival);
                index++;
                break;
            default:
                throw SQLExecError("Insert type is not implemented");
        }
    }
    
    Handle insert_handle = table.insert(&row);
    IndexNames idxn = SQLExec::indices->get_index_names(tbn);
    for(Identifier name : idxn){
        DbIndex& index = SQLExec::indices->get_index(tbn, name);
        index.insert(insert_handle);
    }
    
    return new QueryResult("Successfully inserted 1 row into " + tbn + " and " + to_string(idxn.size()) + " indices");
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier table_name = statement->tableName;
    
    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // get the column names of the table
    ColumnNames cns;
    for (auto const column: table.get_column_names()){
        cns.push_back(column);
    }
    
    // tablescan, when expr is empty
    EvalPlan *plan = new EvalPlan(table);
    
    // filter, when expr is present
    ValueDict* where = new ValueDict;
    if (statement->expr != NULL){
        try{
            where = get_where_conjunction(statement->expr, &cns);
        }
        catch (exception &e){
            throw;
        }
        plan = new EvalPlan(where, plan);
    }
    
    // pipeline results, which is a handle iterator
    EvalPlan *optimized = plan->optimize();
    EvalPipeline pipeline = optimized->pipeline();
    Handles *handles = pipeline.second;
    
    // get all the handles for indices deletion
    auto index_names = SQLExec::indices->get_index_names(table_name);
    size_t index_size = index_names.size();
    size_t handle_size = handles->size();
    
    // delete from indices
    for (auto const& handle : *handles) {
        for (auto const& index : index_names) {
            DbIndex &index_handle = indices->get_index(table_name, index);
            index_handle.del(handle);
        }
        table.del(handle);
    }
    
    // delete from table
    for (auto const& handle: *handles){
        table.del(handle);
    }

    return new QueryResult("successfully deleted " + to_string(handle_size)
                           + " rows from " + table_name + " and " + to_string(index_size) + " indices");
    
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier table_name = statement->fromTable->name;
    
    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);
    
    // get the column names of the table
    ColumnNames cns;
    for (auto const column: table.get_column_names()){
        cns.push_back(column);
    }
    
    // start base of plan at a TableScan
    EvalPlan *plan = new EvalPlan(table);
    
    //enclose that in a select if we have a where clause
    if(statement->whereClause != nullptr)
        plan = new EvalPlan(get_where_conjunction(statement->whereClause, &cns), plan);

    ColumnNames* column_names = new ColumnNames;
    ColumnAttributes * column_attributes = new ColumnAttributes;
    
    //Wrap the whole thing in either ProjectAll or Project
    switch (statement->selectList->front()->type) {
        case kExprStar: {
            // ProjectAll
            *column_names = table.get_column_names();
            *column_attributes = table.get_column_attributes();
            plan = new EvalPlan(EvalPlan::ProjectAll, plan);
            break;
        }
            
        default:{
            //Project for given selectlist
            for(auto const& column : *statement->selectList){
                column_names->push_back(column->name);
            }
            *column_attributes = *table.get_column_attributes(*column_names);
            plan = new EvalPlan(column_names, plan);
            break;
        }
    }
    
    //optimize the plan and evaluate the optimized plan
    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();
    size_t n = rows->size();
    
    return new QueryResult(column_names, column_attributes, rows,
                           "successfuly returned " + to_string(n) + " rows");
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
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

// CREATE ...
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

        } catch (...) {
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
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
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
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
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
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
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
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
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

