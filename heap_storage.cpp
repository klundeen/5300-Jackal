#include "heap_storage.h"
using namespace std;
// =====slottedPage=====

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false) 
{


}

RecordID SlottedPage::add(const Dbt *data)
{

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

void SlottedPage::put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0)
{

}

bool SlottedPage::has_room(u_int16_t size)
{

}

void SlottedPage::slide(u_int16_t start, u_int16_t end)
{

}

u_int16_t SlottedPage::get_n(u_int16_t offset)
{

}

void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{

}

void* SlottedPage::address(u_int16_t offset)
{

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

SlottedPage* HeapFile::get_new(void)
{

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

Dbt* HeapTable::marshal(const ValueDict *row)
{

}

ValueDict* HeapTable::unmarshal(Dbt *data)
{

}
