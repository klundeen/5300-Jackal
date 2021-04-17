#include <iostream>
#include <string>
#include "heap_storage.h"
using namespace std;

typedef uint16_t u_int16_t;

// =============================slottedPage==================================
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false) 
{
    if (is_new) 
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } 
    else 
    {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u_int16_t id = ++this->num_records;
    u_int16_t size = (u_int16_t) data->get_size();
    this->end_free -= size;
    u_int16_t loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id)
{
    u_int16_t size, loc;
    get_header(size, loc, record_id);
    if(loc == 0)
    {
        return nullptr; // ?? None in python
    }
    return new Dbt(this->address(loc), size)
}

void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    u_int16_t new_size = (u_int16_t) data.get_size();
    if(new_size > size)
    {
        u_int16_t extra = new_size - size;
        if(!this->has_room(extra))
        {
            throw DbBlockNoRoomError("Not enough room in block");
        }
        this->slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra), data->get_data(), new_size);
    }
    else
    {
        memcpy(this->address(loc), data->get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, size, loc);
}

void SlottedPage::del(RecordID record_id)
{
    u_int16_t size, loc;
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void)
{
    u_int16_t size, loc;
    RecordID* sequence = new RecordIDs();
    for(u_int16_t i = 1; i < this.num_records + 1; i++)
    {
        get_header(size, loc, i)
        if(loc != 0)
        {
            sequence->push_back(i);
        }
    }
    return sequence;
}

// protected
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0)
{
    this->get_n((u_int16_t)4 * id);
    this->get_n((u_int16_t)(4 * id + 4));
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0)
{
    if(id = 0)
    {
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u_int16_t)4 * id, size);
    put_n((u_int16_t)(4 * id + 2), loc);
}

bool SlottedPage::has_room(u_int16_t size)
{
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
    u_int16_t shift = end - start;
    if(shift == 0)
    {
        return;
    }

    for (int i = 0; i < this.ids->size(); i++)
    {
        u_int16_t size, loc; 
        get_header(size, loc, i);
        if(loc <= start) 
        {
            loc += shift;
            put_header(i, size, loc);
        }   
    }
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u_int16_t SlottedPage::get_n(u_int16_t offset)
{
    return *(u_int16_t*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{
    *(u_int16_t*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u_int16_t offset)
{
    return (void*)((char*)this->block.get_data() + offset);
}


// ===================================HeapFile=======================================
void HeapFile::create(void)
{
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    delete block;
}

// delete in .py
// not sure this one
void HeapFile::drop(void)
{
    this->close();
    this->close = True;
}

void HeapFile::open(void)
{
    this->db_open();
}

void HeapFile::close(void)
{
    this->db.close();
    this->closed = True;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void)
{
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

// not finish!!*****
SlottedPage* HeapFile::get(BlockID block_id)
{
    return SlottedPage(, block_id);
}

// not finish !! need def for db
void HeapFile::put(DbBlock *block)
{
}

BlockIDs* HeapFile::block_ids()
{
    RecordID* sequence = new RecordIDs();
    for(u_int16_t i = 1; i < this->last + 1; i++)
    {
        sequence->push_back(i);
    }
    return sequence;
}

// protected
// not finish !! need def for db
void HeapFile::db_open(uint flags = 0)
{
    if(!this->closed)
    {
        return;
    }
}


// ===========================================Heaptable============================
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
{
    DbRelation(table_name, column_names, column_attributes);
    this->file = HeapFile(table_name);
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
    catch(DbRelationError)
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
    // "Not milestone 2 for HeapTable"
}

Handles* HeapTable::select(const ValueDict *where)
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

ValueDict* HeapTable::project(Handle handle)
{
    // "Not milestone 2 for HeapTable"
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    // "Not milestone 2 for HeapTable"
}

// !!not finish****
// protected
ValueDict* HeapTable::validate(const ValueDict *row)
{
    map<, > full_row;
    for(Identifier column_name:this->column_attributes)
    {
        ColumnAttribute column = this->column_attributes[column_names];
        if(column_name.)
    }
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
    return this->file.get_last_block_id(), record_id
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
ValueDict* HeapTable::unmarshal(Dbt *data)
{
    map<, > row;
    int offset = 0;
    for(Identifier column_name: this->column_names)
    {
        this->co
    }
    
}
