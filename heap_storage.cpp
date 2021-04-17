#include "heap_storage.h"
using namespace std;
// =====slottedPage=====

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

}

void SlottedPage::put(RecordID record_id, const Dbt &data)
{

}

void SlottedPage::del(RecordID record_id)
{

}

RecordIDs* SlottedPage::ids(void)
{

}

// protected
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0)
{

}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0)
{

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


// =====HeapFile=====
void HeapFile::create(void)
{

}

void HeapFile::drop(void)
{

}

void HeapFile::open(void)
{

}

void HeapFile::close(void)
{

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

SlottedPage* HeapFile::get(BlockID block_id)
{

}

void HeapFile::put(DbBlock *block)
{

}

BlockIDs* HeapFile::block_ids()
{

}

// protected
void HeapFile::db_open(uint flags = 0)
{

}


// =====Heaptable=====
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
{

}

void HeapTable::create()
{

}

void HeapTable::create_if_not_exists()
{

}

void HeapTable::drop()
{

}

void HeapTable::open()
{

}

void HeapTable::close()
{

}

Handle HeapTable::insert(const ValueDict *row)
{

}

void HeapTable::update(const Handle handle, const ValueDict *new_values)
{

}

void HeapTable::del(const Handle handle)
{

}

Handles* HeapTable::select()
{

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

}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names)
{

}

// protected
ValueDict* HeapTable::validate(const ValueDict *row)
{

}

Handle HeapTable::append(const ValueDict *row)
{

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

ValueDict* HeapTable::unmarshal(Dbt *data)
{

}
