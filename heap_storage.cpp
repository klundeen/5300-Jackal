#include <iostream>
#include <string>
#include <cstring>
#include "heap_storage.h"
#include "storage_engine.h"
using namespace std;

typedef u_int16_t u16;
typedef u_int32_t u32;

// =============================slottedPage==================================
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room(data->get_size() + 4))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    // not sure why put_header() is called here from python code
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id); 
    if(loc == 0) {
        return nullptr; 
    }
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if(!has_room(extra)) {
            throw DbBlockNoRoomError("Not enough room in block");
        }
        slide(loc, loc - extra); // todo: not sure about this line of code
        memcpy(address(loc-extra), data.get_data(), new_size);
    } else {
        memcpy(address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs *recordIds = new RecordIDs();
    for (int i = 1; i < num_records + 1; i++) {
        u16 size;
        u16 loc;
        get_header(size, loc, i);
        if (size != 0) {
            recordIds->push_back((RecordID)i);
        }
    }
    return recordIds;
}

// protected
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = this->get_n((u16)4 * id);
    loc = this->get_n((u16)(4 * id + 2));
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) {
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u16 size) {
    u16 available = this->end_free - (this->num_records + 2)*4;
    return size <= available;
}

void SlottedPage::slide(u16 start, u16 end) {
    u16 shift = end - start;
    if(shift == 0) {
        return;
    }
    memcpy(address(end_free + 1 + shift), address(end_free + 1), start - end_free + 1);
    for (RecordID id : *ids()) {
        u16 size;
        u16 loc;
        get_header(size, loc, id);
        if (loc <= start) {
            loc += shift;
            put_header(id, size, loc);
        }
    }
    end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


// ===================================HeapFile=======================================
void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    this->put(block);
}

void HeapFile::drop(void) {
    this->open();
    this->close();
    remove(dbfilename.c_str()); // convert to c_str()
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0); //flag = 0
    this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}


// https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/dbget.html
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock *block) {
    u32 blockID = block->get_block_id();
    Dbt key(&blockID, sizeof(blockID));
    db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs* sequence = new BlockIDs();
    for(u32 i = 1; i < this->last + 1; i++)
    {
        sequence->push_back(i);
    }
    return sequence;
}

// protected
// not finish !! need def for db
void HeapFile::db_open(uint flags)
{
    if (!closed) {
        return;
    }
    db.set_re_len(DbBlock::BLOCK_SZ);
    dbfilename = name + ".db";
    db.open(nullptr, dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);
    if (flags == 0) {
        DB_BTREE_STAT stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        this->last = stat.bt_ndata;
    } else {
        this->last = 0;
    }
    closed = false;
}


// ===========================================Heaptable============================
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}
void HeapTable::create()
{
    this->file.create();
}

void HeapTable::create_if_not_exists()
{
    try
    {
        this->open();
    }
    catch(DbException)
    {
        this->create();
    }
}

void HeapTable::drop()
{
    this->file.drop();
}

void HeapTable::open()
{
    this->file.open();
}

void HeapTable::close()
{
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row)
{
    this->open();
    return this->append(row);
}

void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
    // "Not milestone 2 for HeapTable"
}

void HeapTable::del(const Handle handle)
{
    // "Not milestone 2 for HeapTable"
}

Handles* HeapTable::select()
{
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict *where)
{
        
    // "Not milestone 2 for HeapTable"
    // FIX ME
    return nullptr;
}

ValueDict* HeapTable::project(Handle handle)
{
    // "Not milestone 2 for HeapTable"
    // FIX ME
    return nullptr;

}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    // "Not milestone 2 for HeapTable"
    // FIX ME
    return nullptr;
}

// !!not finish****
// protected
ValueDict* HeapTable::validate(const ValueDict *row)
{
    ValueDict* full_row = new ValueDict();
    uint i = 0;
    for(Identifier column_name: column_names)
    {
        ColumnAttribute column = this->column_attributes[i++];
        Value value;
        if(row->find(column_name) == row->end())//column.get_data_type() != ColumnAttribute::DataType::TEXT && column.get_data_type() != ColumnAttribute::DataType::INT)
        {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        }
        else
        {
            value = row->at(column_name);
        }
        full_row->insert(std::pair<Identifier, Value>(column_name, value));
        //full_row[column_name] = value;
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict *row)
{
    Dbt* data = this->marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try
    {
        record_id = block->add(data);
    }
    catch(DbRelationError)
    {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    
    return std::pair<BlockID, RecordID>(this->file.get_last_block_id(), record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u_int16_t*) (bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// !! not finish
ValueDict* HeapTable::unmarshal(Dbt *data) {
    std::map<Identifier, Value> *row = {};
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint i = 0;

    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[i++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            value.n = *(u16 *) (bytes + offset);
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    delete[] bytes;
    return row;    
}

// below method is from: https://seattleu.instructure.com/courses/1597073/files/66811759
// the object in this test code is not immutable. TODO: fix it
bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout <<"create_if_not_exists_ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;

    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12) 
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}